#include <ftw.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "fs.h"
#include "../tracing.h"

int FsEnsureDir(const char *path)
{
	int rv;
	struct stat st = {0};

	rv = stat(path, &st);
	if (rv == 0) {
		if (!S_ISDIR(st.st_mode)) {
			tracef("%s is not a directory", path);
			return -1;
		}
	}

	/* Directory does not exist */
	if (rv == -1) {
		return mkdir(path, 0755);
	}

	return 0;
}


static int fsRemoveDirFilesNftwFn(const char *       path,
                                  const struct stat *sb,
                                  int                type,
                                  struct FTW *       ftwb)
{
	int rv;

	(void)sb;
	(void)type;
	(void)ftwb;

	rv = 0;

	/* Don't remove directory */
	if (S_ISREG(sb->st_mode)) {
		rv = remove(path);
	}

	return rv;
}

int FsRemoveDirFiles(const char *path)
{
	int rv;

	rv = nftw(path, fsRemoveDirFilesNftwFn,
	          10, FTW_DEPTH | FTW_MOUNT | FTW_PHYS);
	return rv;

}
