/* Declarations for a PATRICIA trie.
   Copyright (C) 2006 Morten Hustveit <morten@rashbox.org>

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public
   License as publiched by the Free Software Foundation; either
   version 2 of the License, or (at your option) any later version.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTIBILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   General Public License for more details.

   You should have received a copy of the GNU General Public
   License along with this library; if not, write to the Free
   Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
   02111-1307 USA.  */

#ifndef PATRICIA_H_
#define PATRICIA_H_ 1

#ifdef __cplusplus
extern "C" {
#endif

#define PATRICIA_IDX_BITS 24
#define PATRICIA_OFF_BITS 16

#define PATRICIA_MAX_ENTRIES   ((1 << PATRICIA_IDX_BITS) - 1)
#define PATRICIA_MAX_KEYLENGTH ((1 << PATRICIA_OFF_BITS) / 8 - 1)

struct patricia;

typedef const char* (*patricia_key_callback)(unsigned int idx, void* arg);

/**
 * Creates an empty PATRICIA trie.
 */
struct patricia* patricia_create(patricia_key_callback get_key, void* arg);

/**
 * Defines a key in the PATRICIA trie.
 *
 * Returns the index of the key; 0 at first call, 1 at second, etc.
 */
unsigned int patricia_define(struct patricia* pat, const char* key);

/**
 * Returns the index of a key, as defined by a previous call to patricia_define.
 *
 * May return a false match if `key' does not exist in the trie.  Compare
 * the returned index yourself, which is guaranteed to be in the range
 * previously returned by patrica_define, or equal to (unsigned int) -1.
 */
unsigned int patricia_lookup(const struct patricia* pat, const char* key);

/**
 * Returns first position of element matching given prefix.
 *
 * This function is useful if the keys are stored in alphabetical order.  Like
 * `patricia_lookup', this function may return false matches.
 */
unsigned int patricia_lookup_prefix(const struct patricia* pat, const char* prefix);

/**
 * Removes an entry from the trie.
 *
 * If the `key' does not exist, a random element is removed -- make sure `key'
 * exists.
 */
void patricia_remove(const struct patricia* pat, const char* key);

/**
 * Write a PATRICIA trie to a file descriptor.
 *
 * Returns the number of bytes written, or -1 on error.
 */
int patricia_write(const struct patricia* pat, int fd);

/**
 * Recreate a PATRICIA trie previously written by patricia_write.
 */
void patricia_read(struct patricia* pat, int fd);

/**
 * Recreate a PATRICIA trie previously written by patricia_write.
 *
 * All changes are discarded on remap.  A mapped PATRICIA trie is immutable.
 */
size_t patricia_remap(struct patricia* pat, const void* data);

/**
 * Deallocates all memory associated with a PATRICIA trie.
 */
void patricia_destroy(struct patricia* pat);

#ifndef NDEBUG
#include <stdio.h>

/**
 * Prints a PATRICIA trie to a file (for debugging).
 */
void patricia_print(struct patricia* pat, FILE* f);

#endif

#ifdef __cplusplus
}
#endif

#endif /* !PATRICIA_H_ */
