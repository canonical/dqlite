#include "vfs2.h"

#include <sqlite3.h>

#include <errno.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define VFS2_WAL_FIXED_SUFFIX1 "-xwal1"
#define VFS2_WAL_FIXED_SUFFIX2 "-xwal2"

struct vfs2_data {
	sqlite3_vfs *orig;
};

struct vfs2_file {
	struct sqlite3_file base;
	sqlite3_file *orig;
	int flags;
	union {
		struct {
			sqlite3_filename moving_name;
			char *wal_cur_fixed_name;
			sqlite3_file *wal_prev;
			char *wal_prev_fixed_name;
		} wal;
		struct {
		} db_shm;
	};
};

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

	if ((xfile->flags & SQLITE_OPEN_WAL) && ofst == 0) {
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
		// TODO save the new salts
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

static struct sqlite3_io_methods methods = {
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
	xout->base.pMethods = &methods;
	xout->flags = flags;

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
	} else {
		xout->orig = sqlite3_malloc(data->orig->szOsFile);
		if (xout->orig == NULL) {
			return SQLITE_NOMEM;
		}
		rv = data->orig->xOpen(data->orig, name, xout->orig, flags, out_flags);
		if (rv != SQLITE_OK) {
			return rv;
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
