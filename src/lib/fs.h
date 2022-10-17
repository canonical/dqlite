#ifndef DQLITE_LIB_FS_H
#define DQLITE_LIB_FS_H


/* Create a directory if it does not already exist. */
int FsEnsureDir(const char *path);

/* Removes all files from a directory. */
int FsRemoveDirFiles(const char *path);

#endif /* DQLITE_LIB_FS_H */
