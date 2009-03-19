/*  Functions for jpt's in-memory table.
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
*/

#include <string.h>

#include "jpt_internal.h"

/* We use two binary tree traversal functions to avoid stack overflow
 * on unbalanced trees. */

static void
JPT_memtable_list_all_left(struct JPT_node* n, struct JPT_node*** nodes)
{
  struct JPT_node* top = n->parent;

  while(n->left)
    n = n->left;

  while(n != top)
  {
    if(n->value != (void*) -1)
    {
      **nodes = n;
      ++(*nodes);
    }

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

    if(n->value != (void*) -1)
    {
      **nodes = n;
      ++(*nodes);
    }

    n = n->right;
  }
}

static void
JPT_memtable_list_column_left(struct JPT_node* n, struct JPT_node*** nodes, uint32_t columnidx)
{
  struct JPT_node* top = n->parent;

  while(n->left && n->columnidx >= columnidx)
    n = n->left;

  while(n != top)
  {
    if(n->columnidx == columnidx && n->value != (void*) -1)
    {
      **nodes = n;
      ++(*nodes);
    }

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

    if(n->columnidx == columnidx && n->value != (void*) -1)
    {
      **nodes = n;
      ++(*nodes);
    }

    if(n->columnidx > columnidx)
      return;

    n = n->right;
  }
}

int
JPT_memtable_has_key(struct JPT_info* info, const char* row, uint32_t columnidx)
{
  struct JPT_node* n = info->root;
  int cmp;

  while(n)
  {
    if(columnidx != n->columnidx)
      cmp = columnidx - n->columnidx;
    else
      cmp = strcmp(row, n->row);

    if(cmp < 0)
    {
      if(!n->left)
        return -1;
      else
        n = n->left;
    }
    else if(cmp > 0)
    {
      if(!n->right)
        return -1;
      else
        n = n->right;
    }
    else
    {
      JPT_splay(info, n);

      if(n->value == (void*) -1)
        return -1;

      return 0;
    }
  }

  return -1;
}

int
JPT_memtable_get(struct JPT_info* info, const char* row, uint32_t columnidx,
                 void** value, size_t* value_size, size_t* skip, size_t* max_read,
                 uint64_t* timestamp)
{
  struct JPT_node* n = info->root;
  int cmp;
  size_t i;

  while(n)
  {
    if(columnidx != n->columnidx)
      cmp = columnidx - n->columnidx;
    else
      cmp = strcmp(row, n->row);

    if(cmp < 0)
    {
      if(!n->left)
        return -1;
      else
        n = n->left;
    }
    else if(cmp > 0)
    {
      if(!n->right)
        return -1;
      else
        n = n->right;
    }
    else
    {
      if(n->value == (void*) -1)
        return -1;

      struct JPT_node_data* d = n->next;
      size_t old_size = *value_size;

      *value_size += n->value_size;

      while(d)
      {
        *value_size += d->value_size;
        d = d->next;
      }

      if(max_read)
      {
        size_t amount;

        if(*value_size > *max_read)
          *value_size = *max_read;

        d = n->next;

        if(old_size + n->value_size <= *value_size)
          amount = n->value_size;
        else
          amount = *value_size - old_size;

        memcpy(((char*) *value) + old_size, n->value, amount);
        i = old_size + amount;

        for(; i < *value_size; d = d->next)
        {
          if(i + d->value_size <= *value_size)
            amount = d->value_size;
          else
            amount = *value_size - d->value_size;

          memcpy(((char*) *value) + i, d->value, d->value_size);
          i += amount;
        }
      }
      else
      {
        *value = realloc(*value, *value_size + 1);

        d = n->next;

        memcpy(((char*) *value) + old_size, n->value, n->value_size);
        i = old_size + n->value_size;

        for(; i < *value_size; i += d->value_size, d = d->next)
          memcpy(((char*) *value) + i, d->value, d->value_size);
      }

      if(timestamp)
        *timestamp = n->timestamp;

      JPT_splay(info, n);

      return 0;
    }
  }

  return -1;
}
