@*  Functions for jpt's in-memory table.
    Copyright (C) 2007, 2008, 2009  Morten Hustveit <morten@rashbox.org>

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

@c

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <string.h>

#include "jpt_internal.h"

  @< Functions @>

@ A heavily unbalanced tree, for example a tree that only has left branches,
can easily cause a stack overflow even if it contains just a few thousand
elements.

If we use two binary tree traversal functions, one which can step down a
sequence of left branches without increasing the stack, the other which can
step down step down a sequency of right branches, one would have to create a
deep zig-zag tree to cause a stack overflow.

@< Functions @>=

  static void
  JPT_memtable_list_all_left(struct JPT_node* n, struct JPT_node*** nodes)
  {
    struct JPT_node* top = n->parent;

    while(n->left)
      n = n->left;

    while(n != top)
    {
      @< Add current node to result list, if not removed @>

      if(n->right)
        JPT_memtable_list_all(n->right, nodes);

      n = n->parent;
    }
  }

  void
  JPT_memtable_list_all(struct JPT_node* n, struct JPT_node*** nodes)
  {
    while(n)
    {
      if(n->left)
        JPT_memtable_list_all_left(n->left, nodes);

      @< Add current node to result list, if not removed @>

      n = n->right;
    }
  }

@ These functions work like the "list all" functions, except they filter for a
given column index.

@< Functions @>=

  static void
  JPT_memtable_list_column_left(struct JPT_node* n, struct JPT_node*** nodes, uint32_t columnidx)
  {
    struct JPT_node* top = n->parent;

    while(n->left && n->columnidx >= columnidx)
      n = n->left;

    while(n != top)
    {
      @< Add current node to result list, if correct column and not removed @>

      if(n->right)
        JPT_memtable_list_column(n->right, nodes, columnidx);

      n = n->parent;
    }
  }

  void
  JPT_memtable_list_column(struct JPT_node* n, struct JPT_node*** nodes, uint32_t columnidx)
  {
    while(n)
    {
      if(n->left)
        JPT_memtable_list_column_left(n->left, nodes, columnidx);

      @< Add current node to result list, if correct column and not removed @>

      if(n->columnidx > columnidx)
        return;

      n = n->right;
    }
  }

@ When a value is removed from the tree, its value is set to |(void*) -1|.

@< Add current node to result list, if not removed @>=

  if(n->data.value != (void*) -1)
  {
    **nodes = n;
    ++(*nodes);
  }

@ When a value is removed from the tree, its value is set to |(void*) -1|.

@< Add current node to result list, if correct column and not removed @>=

  if(n->columnidx == columnidx && n->data.value != (void*) -1)
  {
    **nodes = n;
    ++(*nodes);
  }

@ Our primary sorting key is column index, while the secondary is the row name.
Values of |cmp| less than 0 denotes a left branch, values greather than 0
denotes a right branch.  If |cmp| is zero, the current node matches the search
key.

@< Determine branch of search key @>=

  if(columnidx != n->columnidx)
    cmp = columnidx - n->columnidx;
  else
    cmp = strcmp(row, n->row);

@ Values of |cmp| less than 0 denotes a left branch.

@< Left branch: @>=

  if(cmp < 0)

@ Values of |cmp| greater than 0 denotes a right branch.

@< Right branch: @>=

  if(cmp > 0)

@ Finding a given node is very simple, since a splay tree is stored like any
normal binary tree.  We just start at the top, and move to the left or the
right depending on whether the current key is greather than or less than the
search key.

|JPT_memtable_has_key| returns 0 if the key is found, -1 otherwise.

@< Functions @>=

  int
  JPT_memtable_has_key(struct JPT_info* info, const char* row, uint32_t columnidx)
  {
    struct JPT_node* n = info->root;
    int cmp;

    assert(info->is_writing || 0 != pthread_mutex_trylock(&info->memtable_mutex));

    while(n)
    {
      @< Determine branch of search key @>

      @< Left branch: @>
      {
        if(!n->left)
          return -1;

        n = n->left;

        continue;
      }

      @< Right branch: @>
      {
        if(!n->right)
          return -1;

        n = n->right;

        continue;
      }

      JPT_memtable_splay(info, n);

      if(n->data.value == (void*) -1)
        return -1;

      return 0;
    }

    return -1;
  }

@ The |JPT_memtable_get| function tries to find a given key in a tree, and
appends the associated value to the pointers passed as parameters.

@< Functions @>=

  int
  JPT_memtable_get(struct JPT_info* info, const char* row, uint32_t columnidx,
                   void** value, size_t* value_size, size_t* skip, size_t* max_read,
                   uint64_t* timestamp)
  {
    struct JPT_node* n = info->root;
    int cmp;

    assert(info->is_writing || 0 != pthread_mutex_trylock(&info->memtable_mutex));

    while(n)
    {
      struct JPT_node_data* d;
      size_t i;

      @< Determine branch of search key @>

      @< Left branch: @>
      {
        if(!n->left)
          return -1;

        n = n->left;

        continue;
      }

      @< Right branch: @>
      {
        if(!n->right)
          return -1;

        n = n->right;

        continue;
      }

      @< Read value at current node @>

      return 0;
    }

    return -1;
  }

@ If a node has been removed, its value will have been set to |(void*) -1|, as
a sort of tombstone.

@< Read value at current node @>=

  if(n->data.value == (void*) -1)
    return -1;

  i = *value_size;

  @< Determine value size of current node @>

  if(max_read)
  {
    @< Read up to |max_read| from current node @>
  }
  else
  {
    @< Read all data from current node @>
  }

  if(timestamp)
    *timestamp = n->timestamp;

  JPT_memtable_splay(info, n);

@ When reading data from the memtable, the caller can choose whether or not to
read a predetermined number of bytes (upper bound).  When doing so, the caller
is responsible for making sure the target buffer can hold this amount of data.

@< Read up to |max_read| from current node @>=

  size_t amount;

  if(*value_size > *max_read)
    *value_size = *max_read;

  for(d = &n->data; i < *value_size; d = d->next)
  {
    if(i + d->value_size <= *value_size)
      amount = d->value_size;
    else
      amount = *value_size - d->value_size;

    memcpy(((char*) *value) + i, d->value, d->value_size);
    i += amount;
  }

@ When the caller has not set an upper bound on the number of bytes to read, we
have to start by reallocing the buffer to make sure it can hold all the data
we're going to put into it.

@< Read all data from current node @>=

  *value = realloc(*value, *value_size + 1);

  for(d = &n->data; i < *value_size; d = d->next)
  {
    memcpy(((char*) *value) + i, d->value, d->value_size);
    i += d->value_size;
  }

@ When data is appended to an existing node, a |JPT_data_node| is created and
added to a linked list.

@< Determine value size of current node @>=

  d = &n->data;

  do
  {
    *value_size += d->value_size;
    d = d->next;
  }
  while(d);

@ All splay work is performed by |JPT_memtable_splay|.  A call to this function
brings the specified node to the top of the binary tree, reorganizaing the tree
in the process.  The goal of the reorganization is to make the tree more
balanced.

@< Functions @>=

  #define UPDATE_LINK(node, leg, child) \
  do                                    \
  {                                     \
    (node)->leg = (child);              \
    if(child)                           \
      (child)->parent = (node);         \
  }                                     \
  while(0)

  void
  JPT_memtable_splay(struct JPT_info* info, struct JPT_node* n)
  {
    assert(info->is_writing
           || (info->reader_count
               && 0 != pthread_mutex_trylock(&info->memtable_mutex)));

    while(n->parent)
    {
      struct JPT_node* parent = n->parent;
      int is_left_child = (n == parent->left);

      assert(is_left_child ^ (n == parent->right));

      if(!parent->parent)
      {
        @< Splay: Handle root node @>

        break;
      }
      else
      {
        struct JPT_node* gparent = parent->parent;
        int parent_is_left_child = (parent == gparent->left);

        @< Splay: Replace grandparent with current node @>

        if(is_left_child == parent_is_left_child)
        {
          @< Splay: Handle zig-zig case @>
        }
        else
        {
          @< Splay: Handle zig-zag case @>
        }
      }
    }
  }

@ This is the "zig" case.  This is a simple rotation:

     p                         n
    / \                       / \
   n   C  is transformed to  A   p    or the symmetric equivalent.
  / \                           / \
 A   B                         B   C

where `p' is short for `parent'.  Three links are updated.

@< Splay: Handle root node @>=

  assert(parent == info->root);

  if(is_left_child)
  {
    UPDATE_LINK(parent, left, n->right);
    UPDATE_LINK(n, right, parent);

    info->root = n;
    n->parent = 0;
  }
  else
  {
    UPDATE_LINK(parent, right, n->left);
    UPDATE_LINK(n, left, parent);

    info->root = n;
    n->parent = 0;
  }

@ In both the "zig-zig" and "zig-zag" cases, the grandparent must be replaced
with the current node.  The common code for those two cases are found here.

@< Splay: Replace grandparent with current node @>=

  if(gparent->parent)
  {
    if(gparent->parent->left == gparent)
    {
      gparent->parent->left = n;
    }
    else
    {
      assert(gparent->parent->right == gparent);

      gparent->parent->right = n;
    }

    n->parent = gparent->parent;
  }
  else
  {
    info->root = n;

    n->parent = 0;
  }

@ The is a slightly more complicated rotation than the root node:

       g                         n
      / \                       / \
     p   D                     A   p
    / \     is transformed to     / \     or the symmetric equivalent.
   n   C                         B   g
  / \                               / \
 A   B                             C   D

where `p' is short for `parent' and `g' is short for `gparent' (grand-parent).
The grandparent (subtree root) node has already been replaced.

@< Splay: Handle zig-zig case @>=

  if(is_left_child)
  {
    UPDATE_LINK(gparent, left, parent->right);
    UPDATE_LINK(parent, left, n->right);
    UPDATE_LINK(parent, right, gparent);
    UPDATE_LINK(n, right, parent);
  }
  else
  {
    UPDATE_LINK(gparent, right, parent->left);
    UPDATE_LINK(parent, right, n->left);
    UPDATE_LINK(parent, left, gparent);
    UPDATE_LINK(n, left, parent);
  }

@ This is operation flattens the subtree:

       g
      / \                           n
     p   D                        /   \
    / \     is transformed to    p     g    or the symmetric equivalent.
   A   n                        / \   / \
      / \                      A   B C   D
     B   C

where `p' is short for `parent' and `g' is short for `gparent' (grandparent).
The grandparent (subtree root) node has already been replaced.

@< Splay: Handle zig-zag case @>=

  if(!is_left_child)
  {
    UPDATE_LINK(gparent, left, n->right);
    UPDATE_LINK(parent, right, n->left);
    UPDATE_LINK(n, left, parent);
    UPDATE_LINK(n, right, gparent);
  }
  else
  {
    UPDATE_LINK(gparent, right, n->left);
    UPDATE_LINK(parent, left, n->right);
    UPDATE_LINK(n, right, parent);
    UPDATE_LINK(n, left, gparent);
  }

@ The memtable's buffer is lazily allocated.  This is an attempt to avoid
excessive memory usage when a table is opened but never written to.

We round |info->buffer_util| up to a multiple of four, to make sure all
allocations are word aligned.

@< Functions @>=

  static void*
  JPT_memtable_buffer_alloc(struct JPT_info* info, size_t size)
  {
    void* result;

    assert(info->is_writing
           || (info->reader_count
               && 0 != pthread_mutex_trylock(&info->memtable_mutex)));

    if(!info->buffer)
    {
      info->buffer = malloc(info->buffer_size);

      if(!info->buffer)
      {
        asprintf(&JPT_last_error,
                 "Failed to allocate %zu bytes for memtable: %s",
                 info->buffer_size, strerror(errno));

        return 0;
      }
    }

    result = info->buffer + info->buffer_util;
    info->buffer_util = (info->buffer_util + size + 3) & ~3;

    assert(info->buffer_util <= info->buffer_size);

    return result;
  }

@ The |JPT_memtable_create_node| function is a helper function to
|JPT_memtable_insert|.  It allocates memory for a binary tree node, and its
associated value, and initializes all structure members.

The |will_compact| parameters tells whether the calling function will perform a
compaction before it returns.  When this is the case, we do not need to make a
copy of the value.  This also allows us to handle insertion of values larger
than our buffer size.

@< Functions @>=

  static struct JPT_node*
  JPT_memtable_create_node(struct JPT_info* info,
                           const void* row, uint32_t columnidx,
                           const void* value, size_t value_size,
                           int will_compact)
  {
    struct JPT_node* result;

    assert(info->is_writing
           || (info->reader_count
               && 0 != pthread_mutex_trylock(&info->memtable_mutex)));

    result = JPT_memtable_buffer_alloc(info, sizeof(struct JPT_node));
    result->row = JPT_memtable_buffer_alloc(info, strlen(row) + 1);
    result->data.value_size = value_size;
    result->data.next = 0;
    result->parent = 0;
    result->left = 0;
    result->right = 0;
    result->last = 0;
    result->columnidx = columnidx;

    strcpy(result->row, row);

    if(will_compact)
      result->data.value = (char*) value;
    else
    {
      result->data.value = JPT_memtable_buffer_alloc(info, value_size);
      memcpy(result->data.value, value, value_size);
    }

    return result;
  }

@ The |JPT_memtable_insert| function is the function that will be called for
inserting any new data.  Disktables are only written when the memtable is full,
or when any old value is modified.

When replacing an old value with a value of greater length, part of the new
value might go into disktables and part into the memtable.  The former part of
this process will already have appened when |JPT_memtable_insert| is called.
In this case, the |JPT_WRITTEN| flag is set, to help us distinguish betwee

@< Functions @>=

int
JPT_memtable_insert(struct JPT_info* info, const char* row, uint32_t columnidx,
                    const void* value, size_t value_size, uint64_t* timestamp,
                    int flags)
{
  struct JPT_node* n;
  size_t space_needed;
  size_t row_size = strlen(row) + 1;
  int must_compact = 0;
  int cmp;

  assert(info->is_writing);

  @< Calculate needed space, compact or schedule compact if necessary @>
  @< Handle insertion into an empty tree (creating the root node) @>

  n = info->root;

  for(;;)
  {
    @< Determine branch of search key @>

    @< Left branch: @>
    {
      if(!n->left)
      {
        n->left = JPT_memtable_create_node(info, row, columnidx, value,
                                           value_size, must_compact);
        n->left->timestamp = *timestamp;
        n->left->parent = n;
        ++info->node_count;
        ++info->memtable_key_count;
        info->memtable_key_size += strlen(row) + 1;
        info->memtable_value_size += value_size;

        JPT_memtable_splay(info, n->left);

        break;
      }

      n = n->left;

      continue;
    }

    @< Right branch: @>
    {
      if(!n->right)
      {
        n->right = JPT_memtable_create_node(info, row, columnidx, value,
                                            value_size, must_compact);
        n->right->timestamp = *timestamp;
        n->right->parent = n;
        ++info->node_count;
        ++info->memtable_key_count;
        info->memtable_key_size += strlen(row) + 1;
        info->memtable_value_size += value_size;

        JPT_memtable_splay(info, n->right);

        break;
      }

      n = n->right;

      continue;
    }

    if(n->data.value == (void*) -1)
    {
      @< Reuse existing node (value was previously removed) @>
    }
    else if(flags & JPT_APPEND)
    {
      @< Append value to current node @>
    }
    else if(flags & JPT_REPLACE)
    {
      @< Replace value in current node @>
    }
    else
    {
      JPT_memtable_splay(info, n);

      errno = EEXIST;

      return -1;
    }

    JPT_memtable_splay(info, n);

    break;
  }

done:

  if(must_compact)
  {
    if(-1 == JPT_compact(info))
      return -1;

    /* We return 1 to inform the caller that logging is not required */
    return 1;
  }

  return 0;
}

@ @< Calculate needed space, compact or schedule compact if necessary @>=

  space_needed = ((row_size + 3) & ~3)
               + ((sizeof(struct JPT_node) + 3) & ~3);

  if(info->buffer_util + space_needed > info->buffer_size)
  {
    if(!(flags & (JPT_REPLACE | JPT_APPEND)) && 0 == JPT_memtable_has_key(info, row, columnidx))
    {
      errno = EEXIST;

      return -1;
    }
    else if(flags & JPT_REPLACE)
      JPT_memtable_remove(info, row, columnidx);

    if(-1 == JPT_compact(info))
      return -1;
  }

  assert(info->buffer_util + space_needed <= info->buffer_size);

  space_needed += ((value_size + 3) & ~3);

  if(info->buffer_util + space_needed > info->buffer_size)
    must_compact = 1;

@ @< Handle insertion into an empty tree (creating the root node) @>=

  if(!info->root)
  {
    assert(!info->node_count);
    assert(!info->memtable_key_count);
    assert(!info->memtable_key_size);
    assert(!info->memtable_value_size);

    info->root = JPT_memtable_create_node(info, row, columnidx, value, value_size, must_compact);
    info->root->timestamp = *timestamp;
    info->node_count = 1;
    info->memtable_key_count = 1;
    info->memtable_key_size = strlen(row) + 1;
    info->memtable_value_size = value_size;

    goto done;
  }

@ When replacing a previously removed value, we don't need to allocate room for
a new node; we can just just the tombstone of the previous value.

@< Reuse existing node (value was previously removed) @>=

  if(must_compact)
    n->data.value = (char*) value;
  else
  {
    n->data.value = JPT_memtable_buffer_alloc(info, value_size);
    memcpy(n->data.value, value, value_size);
  }

  n->timestamp = *timestamp;
  n->data.value_size = value_size;
  n->data.next = 0;
  n->last = 0;

  info->memtable_value_size += value_size;
  info->memtable_key_size += strlen(row) + 1;
  ++info->node_count;
  ++info->memtable_key_count;

@ To append data to an existing node, we just have to create a new data node
and add it at the end of the linked list belonging to the node.

@< Append value to current node @>=

  struct JPT_node_data* d = JPT_memtable_buffer_alloc(info, sizeof(struct JPT_node_data));

  if(!n->last)
  {
    n->data.next = d;
    n->last = d;
  }
  else
  {
    assert(!n->last->next);
    n->last->next = d;
    n->last = d;
  }

  if(must_compact)
    d->value = (char*) value;
  else
  {
    d->value = JPT_memtable_buffer_alloc(info, value_size);
    memcpy(d->value, value, value_size);
  }

  n->timestamp = *timestamp;
  d->value_size = value_size;
  d->next = 0;

  info->memtable_value_size += value_size;

@ When replacing a value, we start by using any previously allocated space,
then create a new data node for the remaining part of the new value.

@< Replace value in current node @>=

  struct JPT_node_data* d;

  d = &n->data;

  @< Shrink data node if remainder of value fits inside @>
  @< Overwrite data in current data node @>

  n->last = 0;
  d = d->next;

  while(d && value_size)
  {
    @< Shrink data node if remainder of value fits inside @>
    @< Overwrite data in current data node @>

    n->last = d;
    d = d->next;
  }

  @< Clear remaining data nodes starting at |d| @>

  if(!n->last)
    n->data.next = 0;

  if(value_size)
  {
    d = JPT_memtable_buffer_alloc(info, sizeof(struct JPT_node_data));

    if(must_compact)
      d->value = (char*) value;
    else
    {
      d->value = JPT_memtable_buffer_alloc(info, value_size);
      memcpy(d->value, value, value_size);
    }

    d->value_size = value_size;
    d->next = 0;

    info->memtable_value_size += value_size;

    if(n->data.next)
    {
      n->last->next = d;
      n->last = d;
    }
    else
    {
      n->data.next = d;
      n->last = d;
    }
  }
  else if(n->last)
    n->last->next = 0;

  n->timestamp = *timestamp;

@ @< Shrink data node if remainder of value fits inside @>=

  if(d->value_size >= value_size)
  {
    info->memtable_value_size -= d->value_size;
    info->memtable_value_size += value_size;
    d->value_size = value_size;
  }

@ @< Overwrite data in current data node @>=

  memcpy(d->value, value, d->value_size);

  value = (char*) value + d->value_size;
  value_size -= d->value_size;

@ The |JPT_memtable_remove| function finds the node belonging to the given key,
and places a tombstone in its place.  A tombstone is a node with |value| equal
to |(void*) -1|.

@< Functions @>=

int
JPT_memtable_remove(struct JPT_info* info, const char* row, uint32_t columnidx)
{
  struct JPT_node* n;

  assert(info->is_writing);

  n = info->root;

  while(n)
  {
    int cmp;

    @< Determine branch of search key @>

    @< Left branch: @>
    {
      n = n->left;

      continue;
    }

    @< Right branch: @>
    {
      n = n->right;

      continue;
    }

    if(n->data.value != (void*) -1)
    {
      struct JPT_node_data* d = &n->data;

      @< Clear remaining data nodes starting at |d| @>

      info->memtable_key_size -= strlen(row) + 1;
      --info->memtable_key_count;
      --info->node_count;

      n->data.value = (void*) -1;
      n->data.next = 0;

      return 0;
    }

    return -1;
  }

  return -1;
}

@ We keep track of the total data size at all times in order to make the
process of creating disk tables more efficient.  This snippet handles the
updating of the total value size when a value is replaced with a shorter value,
or being removed.

@< Clear remaining data nodes starting at |d| @>=

  while(d)
  {
    info->memtable_value_size -= d->value_size;

    d = d->next;
  }
