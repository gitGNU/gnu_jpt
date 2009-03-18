/*  Header file for jpt library.
    Copyright (C) 2007,2008,2009  Morten Hustveit <morten@rashbox.org>

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef JPT_H_
#define JPT_H_ 1

#include <stdlib.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Flags for jpt_init */
#define JPT_RECOVER 0x0001
#define JPT_SYNC    0x0002

/* Flags for jpt_insert */
#define JPT_IGNORE   0x0000
#define JPT_APPEND   0x0001
#define JPT_REPLACE  0x0002

/* Flags for jpt_remove_column */
#define JPT_REMOVE_IF_EMPTY 0x0001

#define jpt_append(info, row, column, value, value_size) jpt_insert(info, row, column, value, value_size, JPT_APPEND)
#define jpt_replace(info, row, column, value, value_size) jpt_insert(info, row, column, value, value_size, JPT_REPLACE)

/**
 * A table handle.
 *
 * A table consists of one memtable and any amount of disktables.  A memtable
 * holds only the most recently added items, while the disktables holds all
 * other data.
 *
 * Disktables residing in memory cache have lower access times than the memtable.
 */
struct JPT_info;

/**
 * Cell callback prototype.
 *
 * This is the prototype for the function call by functions returning single
 * cells at a time.
 */
typedef int (*jpt_cell_callback)(const char* row, const char* column, const void* data, size_t data_size, uint64_t* timestamp, void* arg);

const char*
jpt_last_error();

/**
 * Opens an existing table or creates a new one.
 *
 * The `buffer_size' parameter indicates the maximum size of the memtable.
 * This is allocated in a single call to malloc.
 */
struct JPT_info*
jpt_init(const char* filename, size_t buffer_size, int flags);

/**
 * Performs a compact and releases any resources held by the given table.
 *
 * This function does not write anything to disk, so if you want your program
 * to exit fast, you do not need to call this function.
 */
void
jpt_close(struct JPT_info* info);

/**
 * Backs up the table to a file.
 *
 * The file format used by this function is very simple, and will be supported
 * by all versions of this library.  If `filename' is "-", standard output will
 * be used.  `mindate' can be used for incremental backups.
 */
int
jpt_backup(struct JPT_info* info, const char* filename, const char* column, uint64_t mindate);

/**
 * Restores a table from a file created by `jpt_backup'.
 *
 * The data will be merged according to the `flags' parameter.  See
 * `jpt_insert' for details.  If `filename' is "-", standard input will be used.
 */
int
jpt_restore(struct JPT_info* info, const char* filename, int flags);

/**
 * Converts the memtable into a disktable.
 *
 * This is usually only needed to free up memory for more inserts.
 */
int
jpt_compact(struct JPT_info* info);

/**
 * Converts the memtable and all disktables into as few disktables as possible.
 *
 * This is the only function that actually frees up the space used by removed
 * elements.  It also ensures that access to any element in the table requires
 * no more than one seek per disktable.
 *
 * This function is never called implicitly.  The execution time can be long.
 */
int
jpt_major_compact(struct JPT_info* info);

/**
 * Inserts data into a given cell.
 *
 * This function potentially calls `jpt_compact' to ensure there is
 * enough available space in the memtable.
 *
 * `flags' can be either JPT_IGNORE, JPT_APPEND or JPT_REPLACE, and
 * denotes how the value is merged the cell already exists.
 */
int
jpt_insert(struct JPT_info* info,
           const char* row, const char* column,
           const void* value, size_t value_size, int flags);

int
jpt_insert_timestamp(struct JPT_info* info,
                     const char* row, const char* column,
                     const void* value, size_t value_size,
                     uint64_t* timestamp, int flags);

/**
 * Removes the data in a given cell.
 */
int
jpt_remove(struct JPT_info* info, const char* row, const char* column);

/**
 * Removes the data in a given column.
 */
int
jpt_remove_column(struct JPT_info* info, const char* column, int flags);

/**
 * Create the given column.
 *
 * Columns are implicitly created on insert aswell.
 */
int
jpt_create_column(struct JPT_info* info, const char* column, int flags);

/**
 * Returns 0 if the cell is found, -1 otherwise.
 *
 * errno will not be touched.
 */
int
jpt_has_key(struct JPT_info* info, const char* row, const char* column);

/**
 * Returns 0 if the column is found, -1 otherwise.
 *
 * errno will not be touched.
 */
int
jpt_has_column(struct JPT_info* info, const char* column);

/**
 * Retrieves a value from from a given cell.
 *
 * The value is allocated using malloc, and must be freed by the caller.
 *
 * For convenience, a NUL byte is always placed just beyond the end of the
 * value, in case you are trying to fetch a NUL-terminated string.
 */
int
jpt_get(struct JPT_info* info, const char* row, const char* column,
        void** value, size_t* value_size);

/**
 * Retrieves a value from from a given cell, including timestamp.
 *
 * The value is allocated using malloc, and must be freed by the caller.
 */
int
jpt_get_timestamp(struct JPT_info* info, const char* row, const char* column,
                  void** value, size_t* value_size, uint64_t* timestamp);

/**
 * Retrieves a value from from a given cell into a preallocated buffer.
 */
int
jpt_get_fixed(struct JPT_info* info, const char* row, const char* column,
              void* value, size_t value_size);

/**
 * Calls a function for every cell in the table.
 *
 * The cells are returned in sorted order, one column at a time.
 */
int
jpt_scan(struct JPT_info* info, jpt_cell_callback callback, void* arg);

/**
 * Calls a function for every cell in a column.
 *
 * The cells are returned in sorted order.
 */
int
jpt_column_scan(struct JPT_info* info, const char* column,
                jpt_cell_callback callback, void* arg);

/**
 * Retrieves and increments a 64 bit unsigned counter.
 *
 * The counter is initialized to 0 if it does not exist.
 * On error, (uint64_t) ~0ULL is returned, and errno is set.
 */
uint64_t
jpt_get_counter(struct JPT_info* info, const char* name);

uint64_t
jpt_gettime();

#ifdef __cplusplus
}
#endif

#endif /* !JPT_H_ */
