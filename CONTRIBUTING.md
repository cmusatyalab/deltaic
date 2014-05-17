# Designing and implementing a backup source

The existing backup sources have been designed with several principles in
mind.  When adding a new backup source or extending an existing one,
please adhere to them wherever possible.

**Simple storage formats.**  Keep the on-disk storage format simple.  Store
objects in reasonably fine granularity, preferably the natural granularity
of the data source.  Ideally, data should be stored in a native format for
the data type: files and directories, disk images in raw format, etc.  Never
store object metadata in a database; use extended attributes where possible
and small JSON files otherwise.  Ideally, restoring a backup should be
possible with standard tools (e.g. `rsync`) rather than dedicated restore
code.

**Efficient transfer.**  Use available mechanisms to transfer only the
changes since the last backup; avoid transferring large amounts of redundant
data.  If it is necessary to occasionally perform a "full backup" (e.g., to
check for deletions), invoke this more-expensive mode infrequently and
probabilistically.  This will roughly equalize the system load from day to
day and avoids the need to track additional state.

**Atomic updates.**  Commit only clean data; leave no half-written backup
files.  The backup operator shouldn't need to check the logs to see whether
a particular backup was successful before restoring from it.  Use
`UpdateFile` and `update_file` to atomically update file data.  (However, at
present we assume that the backup server will not crash, so you don't need
to use `fsync`.)

**No unnecessary writes.**  Assume that all backup data is subject to
copy-on-write by the LVM layer.  Avoid unnecessary COW by writing data or
metadata only if it has changed.  Never unconditionally update an `mtime`
"just to be sure"; `stat` the file first to check its current `mtime`. 
Likewise with extended attributes.  Use `UpdateFile`, `update_file`, and the
`XAttrs` wrapper class to conditionally update file data and xattrs.

**Periodic scrubbing.**  Scrubbing a backup (checking its data against the
original) should be supported, and should be invoked probabilistically.  If
scrub finds an object mismatch that could be attributed to legitimate
activity (for example, a file was updated without changing its `mtime`),
it's best to transparently update the object.  If this is not reasonable or
safe, or if it is clear that data corruption has occurred in the backup
system (for example, checksums do not match), scrubbing should raise an
exception.
