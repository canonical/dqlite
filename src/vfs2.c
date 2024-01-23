#include "vfs2.h"

#include "lib/queue.h"

#include <pthread.h>
#include <sqlite3.h>

#include <assert.h>
#include <errno.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define VFS2_WAL_FIXED_SUFFIX1 "-xwal1"
#define VFS2_WAL_FIXED_SUFFIX2 "-xwal2"

#define VFS2_WAL_HDR_SIZE 32
#define VFS2_WAL_INDEX_REGION_SIZE (1 << 15)

struct vfs2_data {
	sqlite3_vfs *orig;
	pthread_rwlock_t rwlock;
	uint32_t page_size;
	queue queue;
};

struct vfs2_db_entry {
	struct vfs2_file *db;
	struct vfs2_file *wal;
	queue queue;
};

struct vfs2_wal_frame_hdr {
	uint32_t page_number;
	uint32_t db_size;
	uint32_t salt[2];
	uint32_t checksum[2];
};

struct vfs2_wal_index_basic_hdr {
	uint32_t iVersion;
	uint8_t unused[4];
	uint32_t iChange;
	uint8_t isInit;
	uint8_t bigEndCksum;
	uint16_t szPage;
	uint32_t mxFrame;
	uint32_t nPage;
	uint32_t aFrameCksum[2];
	uint32_t aSalt[2];
	uint32_t aCksum[2];
};

union vfs2_shm_region0 {
	struct vfs2_wal_index_basic_hdr hdr[2];
	char bytes[VFS2_WAL_INDEX_REGION_SIZE];
};

struct vfs2_file {
	struct sqlite3_file base;
	sqlite3_file *orig;
	struct vfs2_data *vfs_data;
	int flags;
	union {
		struct {
			sqlite3_filename moving_name;
			char *wal_cur_fixed_name;
			sqlite3_file *wal_prev;
			char *wal_prev_fixed_name;

			uint32_t pending_txn_start;
			uint32_t pending_txn_len;
		} wal;
		struct {
			sqlite3_filename name;

			struct vfs2_wal_index_basic_hdr prev_txn_hdr;
			struct vfs2_wal_index_basic_hdr pending_txn_hdr;

			void **all_regions;
			unsigned all_regions_len;
			unsigned refcount;
			unsigned shared[SQLITE_SHM_NLOCK];
			unsigned exclusive[SQLITE_SHM_NLOCK];
		} db_shm;
	};
};

static struct vfs2_file *get_partner_file(struct vfs2_file *f) {
	pthread_rwlock_rdlock(&f->vfs_data->rwlock);
	queue *q;
	struct vfs2_file *res = NULL;
	QUEUE__FOREACH(q, &f->vfs_data->queue) {
		struct vfs2_db_entry *entry = QUEUE__DATA(q, struct vfs2_db_entry, queue);
		if (entry->db == f) {
			res = entry->wal;
			break;
		} else if (entry->wal == f) {
			res = entry->db;
			break;
		}

	}
	pthread_rwlock_unlock(&f->vfs_data->rwlock);
	return res;
}

static int vfs2_close(sqlite3_file *file) {
	int rv, rvprev;
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	rvprev = 0;
	if (xfile->flags & SQLITE_OPEN_WAL) {
		sqlite3_free(xfile->wal.wal_cur_fixed_name);
		sqlite3_free(xfile->wal.wal_prev_fixed_name);
		rvprev = xfile->wal.wal_prev->pMethods->xClose(xfile->wal.wal_prev);
	}
	rv = xfile->orig->pMethods->xClose(xfile->orig);
	if (rv != 0) {
		return rv;
	}
	return rvprev;
}

static int vfs2_read(sqlite3_file *file, void *buf, int amt, sqlite3_int64 ofst) {
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	return xfile->orig->pMethods->xRead(xfile->orig, buf, amt, ofst);
}

static int vfs2_write(sqlite3_file *file, const void *buf, int amt, sqlite3_int64 ofst) {
	int rv;
	struct vfs2_file *xfile = (struct vfs2_file *)file;

	if (xfile->flags & SQLITE_OPEN_WAL) {
		if (ofst == 0) {
			assert(amt == VFS2_WAL_HDR_SIZE);
			sqlite3_file *tmp = xfile->orig;
			char *tmp_name = xfile->wal.wal_cur_fixed_name;
			xfile->orig = xfile->wal.wal_prev;
			xfile->wal.wal_cur_fixed_name = xfile->wal.wal_prev_fixed_name;
			xfile->wal.wal_prev = tmp;
			xfile->wal.wal_prev_fixed_name = tmp_name;
			rv = unlink(xfile->wal.moving_name);
			if (rv != 0) {
				// TODO
			}
			rv = link(xfile->wal.wal_cur_fixed_name, xfile->wal.moving_name);
			if (rv != 0) {
				// TODO
			}

			xfile->wal.pending_txn_start = 0;
			xfile->wal.pending_txn_len = 0;

			struct vfs2_file *db = get_partner_file(xfile);
			assert(db->db_shm.all_regions_len > 0);
			union vfs2_shm_region0 *region0 = db->db_shm.all_regions[0];
			db->db_shm.prev_txn_hdr = region0->hdr[0];
		} else if (amt == sizeof(struct vfs2_wal_frame_hdr)) {
			xfile->wal.pending_txn_len++;
		}
	}
	return xfile->orig->pMethods->xWrite(xfile->orig, buf, amt, ofst);
}

static int vfs2_truncate(sqlite3_file *file, sqlite3_int64 size) {
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	return xfile->orig->pMethods->xTruncate(xfile->orig, size);
}

static int vfs2_sync(sqlite3_file *file, int flags) {
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	return xfile->orig->pMethods->xSync(xfile->orig, flags);
}

static int vfs2_file_size(sqlite3_file *file, sqlite3_int64 *size) {
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	return xfile->orig->pMethods->xFileSize(xfile->orig, size);
}

static int vfs2_lock(sqlite3_file *file, int mode) {
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	return xfile->orig->pMethods->xLock(xfile->orig, mode);
}

static int vfs2_unlock(sqlite3_file *file, int mode) {
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	return xfile->orig->pMethods->xUnlock(xfile->orig, mode);
}

static int vfs2_check_reserved_lock(sqlite3_file *file, int *out) {
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	return xfile->orig->pMethods->xCheckReservedLock(xfile->orig, out);
}

static int vfs2_file_control(sqlite3_file *file, int op, void *arg) {
	struct vfs2_file *xfile = (struct vfs2_file *)file;

	if (op == SQLITE_FCNTL_COMMIT_PHASETWO) {
		assert(xfile->flags & SQLITE_OPEN_MAIN_DB);
		assert(xfile->db_shm.all_regions_len > 0);
		union vfs2_shm_region0 *region0 = xfile->db_shm.all_regions[0];
		xfile->db_shm.pending_txn_hdr = region0->hdr[0];
		region0->hdr[0] = xfile->db_shm.prev_txn_hdr;
		region0->hdr[1] = region0->hdr[0];
	}

	return xfile->orig->pMethods->xFileControl(xfile->orig, op, arg);
}

static int vfs2_sector_size(sqlite3_file *file) {
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	return xfile->orig->pMethods->xSectorSize(xfile->orig);
}

static int vfs2_device_characteristics(sqlite3_file *file) {
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	return xfile->orig->pMethods->xDeviceCharacteristics(xfile->orig);
}

static int vfs2_fetch(sqlite3_file *file, sqlite3_int64 ofst, int amt, void **out) {
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	return xfile->orig->pMethods->xFetch(xfile->orig, ofst, amt, out);
}

static int vfs2_unfetch(sqlite3_file *file, sqlite3_int64 ofst, void *buf) {
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	return xfile->orig->pMethods->xUnfetch(xfile->orig, ofst, buf);
}

static int vfs2_shm_map(sqlite3_file *file, int pgno, int pgsz, int extend, void volatile **out) {
	int rv;
	void *region;
	struct vfs2_file *xfile = (struct vfs2_file *)file;

	if (xfile->db_shm.all_regions != NULL && pgno < xfile->db_shm.all_regions_len) {
		region = xfile->db_shm.all_regions[pgno];
		assert(region != NULL);
	} else if (extend) {
		void **regions;

		assert(pgsz == VFS2_WAL_INDEX_REGION_SIZE);
		assert(pgno == xfile->db_shm.all_regions_len);
		region = sqlite3_malloc(pgsz);
		if (region == NULL) {
			rv = SQLITE_NOMEM;
			goto err;
		}

		memset(region, 0, pgsz);

		regions = sqlite3_realloc(xfile->db_shm.all_regions, sizeof(*xfile->db_shm.all_regions) * (xfile->db_shm.all_regions_len + 1));
		if (regions == NULL) {
			rv = SQLITE_NOMEM;
			goto err_after_region_malloc;
		}

		xfile->db_shm.all_regions = regions;
		xfile->db_shm.all_regions[pgno] = region;
		xfile->db_shm.all_regions_len++;
	} else {
		region = NULL;
	}

	*out = region;

	if (pgno == 0 && region != NULL) {
		xfile->db_shm.refcount++;
	}

err_after_region_malloc:
	sqlite3_free(region);
err:
	assert(rv != SQLITE_OK);
	*out = NULL;
	return rv;
}

static int vfs2_shm_lock(sqlite3_file *file, int ofst, int n, int flags) {
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	int i;

	if (flags & SQLITE_SHM_EXCLUSIVE) {
		for (i = ofst; i < ofst + n; i++) {
			if (xfile->db_shm.shared[i] > 0 || xfile->db_shm.exclusive[i] > 0) {
				return SQLITE_BUSY;
			}
		}

		for (i = ofst; i < ofst + n; i++) {
			assert(xfile->db_shm.exclusive[i] == 0);
			xfile->db_shm.exclusive[i] = 1;
		}
	} else {
		for (i = ofst; i < ofst + n; i++) {
			if (xfile->db_shm.exclusive[i] > 0) {
				return SQLITE_BUSY;
			}
		}

		for (i = ofst; i < ofst + n; i++) {
			xfile->db_shm.shared[i]++;
		}
	}

	return SQLITE_OK;
}

static void vfs2_shm_barrier(sqlite3_file *file) {
	(void)file;
}

static int vfs2_shm_unmap(sqlite3_file *file, int delete) {
	(void)delete;
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	xfile->db_shm.refcount--;
	if (xfile->db_shm.refcount == 0) {
		for (int i = 0; i < xfile->db_shm.all_regions_len; i++) {
			void *region = xfile->db_shm.all_regions[i];
			assert(region != NULL);
			sqlite3_free(region);
		}
		sqlite3_free(xfile->db_shm.all_regions);

		xfile->db_shm.all_regions = NULL;
		xfile->db_shm.all_regions_len = 0;
		for (int i = 0; i < SQLITE_SHM_NLOCK; i++) {
			xfile->db_shm.shared[i] = 0;
			xfile->db_shm.exclusive[i] = 0;
		}
	}
	return SQLITE_OK;
}

static struct sqlite3_io_methods vfs2_io_methods = {
	3,
	vfs2_close,
	vfs2_read,
	vfs2_write,
	vfs2_truncate,
	vfs2_sync,
	vfs2_file_size,
	vfs2_lock,
	vfs2_unlock,
	vfs2_check_reserved_lock,
	vfs2_file_control,
	vfs2_sector_size,
	vfs2_device_characteristics,
	vfs2_shm_map,
	vfs2_shm_lock,
	vfs2_shm_barrier,
	vfs2_shm_unmap,
	vfs2_fetch,
	vfs2_unfetch
};

static int vfs2_open(sqlite3_vfs *vfs, sqlite3_filename name, sqlite3_file *out, int flags, int *out_flags) {
	int rv;
	out->pMethods = NULL;
	*out_flags = 0;
	struct vfs2_file *xout = (struct vfs2_file *)out;
	struct vfs2_data *data = vfs->pAppData;
	xout->base.pMethods = &vfs2_io_methods;
	xout->flags = flags;
	xout->vfs_data = data;

	if (flags & SQLITE_OPEN_WAL) {
		const char *dbname = sqlite3_filename_database(name);
		if (strlen(dbname) + strlen(VFS2_WAL_FIXED_SUFFIX1) > data->orig->mxPathname) {
			return SQLITE_ERROR;
		}
		char *fixed1 = sqlite3_malloc(data->orig->mxPathname + 1);
		char *fixed2 = sqlite3_malloc(data->orig->mxPathname + 1);
		sqlite3_file *phys1 = sqlite3_malloc(data->orig->szOsFile);
		sqlite3_file *phys2 = sqlite3_malloc(data->orig->szOsFile);
		if (fixed1 == NULL || fixed2 == NULL || phys1 == NULL || phys2 == NULL) {
			return SQLITE_NOMEM;
		}
		strcpy(fixed1, dbname);
		strcat(fixed1, VFS2_WAL_FIXED_SUFFIX1);
		strcpy(fixed2, dbname);
		strcat(fixed2, VFS2_WAL_FIXED_SUFFIX2);
		int out_flags1;
		rv = data->orig->xOpen(data->orig, fixed1, phys1, flags, &out_flags1);
		if (rv != SQLITE_OK) {
			// TODO
		}
		int out_flags2;
		rv = data->orig->xOpen(data->orig, fixed2, phys2, flags, &out_flags2);
		if (rv != SQLITE_OK) {
			// TODO
		}
		struct stat s1, s2;
		int rv1 = stat(fixed1, &s1);
		int rv2 = stat(fixed2, &s2);
		if (rv1 != 0 || rv2 != 0) {
			// TODO
		}
		struct stat s;
		rv = stat(name, &s);
		if (rv != 0 && errno == ENOENT) {
			xout->orig = phys1;
			xout->wal.moving_name = name;
			xout->wal.wal_cur_fixed_name = fixed1;
			xout->wal.wal_prev = phys2;
			xout->wal.wal_prev_fixed_name = fixed2;
		} else if (rv != 0) {
			// TODO
		} else if (s.st_ino == s1.st_ino) {
			xout->orig = phys1;
			xout->wal.moving_name = name;
			xout->wal.wal_cur_fixed_name = fixed1;
			xout->wal.wal_prev = phys2;
			xout->wal.wal_prev_fixed_name = fixed2;
		} else if (s.st_ino == s2.st_ino) {
			xout->orig = phys2;
			xout->wal.moving_name = name;
			xout->wal.wal_cur_fixed_name = fixed2;
			xout->wal.wal_prev = phys1;
			xout->wal.wal_prev_fixed_name = fixed1;
		} else {
			// TODO
		}

		{
			pthread_rwlock_wrlock(&data->rwlock);
			queue *q;
			struct vfs2_db_entry *entry;
			bool found = false;
			QUEUE__FOREACH(q, &data->queue) {
				entry = QUEUE__DATA(q, struct vfs2_db_entry, queue);
				if (entry->db != NULL && strcmp(entry->db->db_shm.name, sqlite3_filename_database(name)) == 0) {
					found = true;
					break;
				}
			}
			if (!found) {
				entry = sqlite3_malloc(sizeof(*entry));
				if (entry == NULL) {
					// TODO
				}
				entry->db = NULL;
				QUEUE__PUSH(&data->queue, &entry->queue);
			}
			entry->wal = xout;
			pthread_rwlock_unlock(&data->rwlock);
		}
	} else if (flags & SQLITE_OPEN_MAIN_DB) {
		xout->orig = sqlite3_malloc(data->orig->szOsFile);
		if (xout->orig == NULL) {
			return SQLITE_NOMEM;
		}
		rv = data->orig->xOpen(data->orig, name, xout->orig, flags, out_flags);
		if (rv != SQLITE_OK) {
			return rv;
		}

		xout->db_shm.name = name;
		xout->db_shm.all_regions = NULL;
		xout->db_shm.all_regions_len = 0;
		xout->db_shm.refcount = 0;
		memset(xout->db_shm.shared, 0, sizeof(xout->db_shm.shared));
		memset(xout->db_shm.exclusive, 0, sizeof(xout->db_shm.exclusive));

		{
			pthread_rwlock_wrlock(&data->rwlock);
			queue *q;
			struct vfs2_db_entry *entry;
			bool found = false;
			QUEUE__FOREACH(q, &data->queue) {
				entry = QUEUE__DATA(q, struct vfs2_db_entry, queue);
				if (entry->wal != NULL && strcmp(entry->wal->wal.moving_name, sqlite3_filename_wal(name)) == 0) {
					found = true;
					break;
				}
			}
			if (!found) {
				entry = sqlite3_malloc(sizeof(*entry));
				if (entry == NULL) {
					// TODO
				}
				entry->wal = NULL;
				QUEUE__PUSH(&data->queue, &entry->queue);
			}
			entry->db = xout;
			pthread_rwlock_unlock(&data->rwlock);
		}
	}

	return SQLITE_OK;
}

static int vfs2_delete(sqlite3_vfs *vfs, const char *name, int sync_dir) {
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xDelete(data->orig, name, sync_dir);
}

static int vfs2_access(sqlite3_vfs *vfs, const char *name, int flags, int *out) {
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xAccess(data->orig, name, flags, out);
}

static int vfs2_full_pathname(sqlite3_vfs *vfs, const char *name, int n, char *out) {
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xFullPathname(data->orig, name, n, out);
}

static void *vfs2_dl_open(sqlite3_vfs *vfs, const char *filename) {
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xDlOpen(data->orig, filename);
}

static void vfs2_dl_error(sqlite3_vfs *vfs, int n, char *msg) {
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xDlError(data->orig, n, msg);
}

typedef void (*vfs2_sym)(void);
static vfs2_sym vfs2_dl_sym(sqlite3_vfs *vfs, void *dl, const char *symbol) {
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xDlSym(data->orig, dl, symbol);
}

static void vfs2_dl_close(sqlite3_vfs *vfs, void *dl) {
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xDlClose(data->orig, dl);
}

static int vfs2_randomness(sqlite3_vfs *vfs, int n, char *out) {
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xRandomness(data->orig, n, out);
}

static int vfs2_sleep(sqlite3_vfs *vfs, int microseconds) {
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xSleep(data->orig, microseconds);
}

static int vfs2_current_time(sqlite3_vfs *vfs, double *out) {
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xCurrentTime(data->orig, out);
}

static int vfs2_get_last_error(sqlite3_vfs *vfs, int n, char *out) {
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xGetLastError(data->orig, n, out);
}

static int vfs2_current_time_int64(sqlite3_vfs *vfs, sqlite3_int64 *out) {
	struct vfs2_data *data = vfs->pAppData;
	if (data->orig->iVersion < 2) {
		return SQLITE_ERROR;
	}
	return data->orig->xCurrentTimeInt64(data->orig, out);
}

sqlite3_vfs *vfs2_make(sqlite3_vfs *orig) {
	struct vfs2_data *data = sqlite3_malloc(sizeof(*data));
	if (data == NULL) {
		goto err;
	}
	data->orig = orig;
	struct sqlite3_vfs *vfs = sqlite3_malloc(sizeof(*vfs));
	if (vfs == NULL) {
		goto err_after_alloc_data;
	}
	vfs->iVersion = 2;
	vfs->szOsFile = sizeof(struct vfs2_file);
	vfs->mxPathname = orig->mxPathname;
	vfs->zName = "dqlite-vfs2";
	vfs->pAppData = data;
	vfs->xOpen = vfs2_open;
	vfs->xDelete = vfs2_delete;
	vfs->xAccess = vfs2_access;
	vfs->xFullPathname = vfs2_full_pathname;
	vfs->xDlOpen = vfs2_dl_open;
	vfs->xDlError = vfs2_dl_error;
	vfs->xDlSym = vfs2_dl_sym;
	vfs->xDlClose = vfs2_dl_close;
	vfs->xRandomness = vfs2_randomness;
	vfs->xSleep = vfs2_sleep;
	vfs->xCurrentTime = vfs2_current_time;
	vfs->xGetLastError = vfs2_get_last_error;
	vfs->xCurrentTimeInt64 = vfs2_current_time_int64;
	return vfs;

err_after_alloc_data:
	sqlite3_free(data);
err:
	return NULL;
}

int vfs2_apply(sqlite3_file *file) {
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	if (!(xfile->flags & SQLITE_OPEN_MAIN_DB)) {
		return 1;
	}
	if (xfile->db_shm.all_regions_len == 0) {
		return 1;
	}
	union vfs2_shm_region0 *region0 = xfile->db_shm.all_regions[0];
	region0->hdr[0] = xfile->db_shm.pending_txn_hdr;
	xfile->db_shm.prev_txn_hdr = xfile->db_shm.pending_txn_hdr;
	return 0;
}
