#ifndef DJPT_H_
#define DJPT_H_ 1

#include <stdlib.h>
#include <stdint.h>

/* Flags for djpt_insert */
#define DJPT_IGNORE        0x00
#define DJPT_APPEND        0x01
#define DJPT_REPLACE       0x02
#define DJPT_IGNORE_RESULT 0x40

/* Flags for djpt_remove_column */
#define DJPT_REMOVE_IF_EMPTY 0x0001

struct DJPT_info;

typedef int (*djpt_cell_callback)(const char* row, const char* column, const void* data, size_t data_size, uint64_t* timestamp, void* arg);
typedef int (*djpt_eval_callback)(const void* data, size_t data_size, void* arg);

const char*
djpt_last_error();

struct DJPT_info*
djpt_init(const char* database);

void
djpt_close(struct DJPT_info* info);

int
djpt_insert(struct DJPT_info* info,
            const char* row, const char* column,
            const void* value, size_t value_size, int flags);

int
djpt_remove(struct DJPT_info* info, const char* row, const char* column);

int
djpt_remove_column(struct DJPT_info* info, const char* column, int flags);

int
djpt_create_column(struct DJPT_info* info, const char* column, int flags);

int
djpt_has_key(struct DJPT_info* info, const char* row, const char* column);

int
djpt_has_column(struct DJPT_info* info, const char* column);

int
djpt_get(struct DJPT_info* info, const char* row, const char* column,
         void** value, size_t* value_size);

int
djpt_get_fixed(struct DJPT_info* info, const char* row, const char* column,
               void* value, size_t value_size);

int
djpt_scan(struct DJPT_info* info, djpt_cell_callback callback, void* arg);

int
djpt_column_scan(struct DJPT_info* info, const char* column,
                 djpt_cell_callback callback, void* arg,
                 size_t limit);

int
djpt_eval(struct DJPT_info* info, const char* program,
          djpt_eval_callback callback, void* arg);

uint64_t
djpt_get_counter(struct DJPT_info* info, const char* name);

int
djpt_compact(struct DJPT_info* info);

int
djpt_major_compact(struct DJPT_info* info);

#endif /* !DJPT_H_ */
