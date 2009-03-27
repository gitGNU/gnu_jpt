/*  Implementation of a binary PATRICIA trie.
    Copyright (C) 2006,2007,2008  Morten Hustveit <morten@rashbox.org>

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

#if HAVE_CONFIG_H
#include "config.h"
#endif

#include <assert.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

#include "patricia.h"


struct pat_node
{
  unsigned right  : PATRICIA_IDX_BITS __attribute__((packed));
  unsigned left   : PATRICIA_IDX_BITS __attribute__((packed));
  unsigned bitidx : PATRICIA_OFF_BITS __attribute__((packed));
};

struct patricia
{
  patricia_key_callback get_key;
  void* arg;

  unsigned int count;
  unsigned int capacity;
  struct pat_node* nodes;

  int mapped;
};

#define getbit(key, idx) ((key)[(idx - 1) >> 3] & (1 << ((idx - 1) & 7)))

struct patricia* patricia_create(patricia_key_callback get_key, void* arg)
{
  struct patricia* result = (struct patricia*) malloc(sizeof(struct patricia));

  result->get_key = get_key;
  result->arg = arg;

  result->count = 1;
  result->capacity = sizeof(struct pat_node);
  result->nodes = (struct pat_node*) malloc(result->capacity);
  result->nodes[0].left = 0;
  result->nodes[0].right = 0;
  result->nodes[0].bitidx = 0;

  result->mapped = 0;

  return result;
}

unsigned int patricia_define(struct patricia* pat, const char* key)
{
  struct pat_node* node = pat->nodes;
  struct pat_node* next = pat->nodes + node->right;
  int maxbit = (strlen(key) + 1) * 8;

  assert(*key);
  assert(pat->count <= PATRICIA_MAX_ENTRIES);
  assert(strlen(key) <= PATRICIA_MAX_KEYLENGTH);

  /* Perform a lookup on the word to be defined. */
  while(node->bitidx < next->bitidx)
  {
    node = next;

    if(next->bitidx < maxbit && getbit(key, next->bitidx))
      next = pat->nodes + next->right;
    else
      next = pat->nodes + next->left;
  }

  /* Retrieve the key at the corresponding index, or use the empty string if no
   * match was found. */
  const char* nkey;

  if(next != pat->nodes)
    nkey = pat->get_key(next - pat->nodes - 1, pat->arg);
  else
    nkey = "";

  /* Compare the new and existing keys, and remember the index of the first
   * differing character. */
  int idx = 0;

  while(key[idx] && key[idx] == nkey[idx])
    ++idx;

  if(!key[idx] && !nkey[idx])
    return next - pat->nodes - 1;

  if((pat->count + 1) * sizeof(struct pat_node) > pat->capacity)
  {
    pat->capacity = pat->capacity * 3 / 2 + sizeof(struct pat_node);
    pat->nodes = (struct pat_node*) realloc(pat->nodes, pat->capacity);
  }

  int bitidx = (idx << 3) + ffs(key[idx] ^ nkey[idx]);

  assert(bitidx < maxbit);

  node = pat->nodes;
  next = pat->nodes + node->right;

  while(node->bitidx < next->bitidx && next->bitidx < bitidx)
  {
    node = next;

    if(getbit(key, next->bitidx))
      next = pat->nodes + next->right;
    else
      next = pat->nodes + next->left;
  }

  struct pat_node* new_node = pat->nodes + pat->count;

  if(getbit(key, bitidx))
  {
    new_node->left = next - pat->nodes;
    new_node->right = pat->count;
  }
  else
  {
    new_node->left = pat->count;
    new_node->right = next - pat->nodes;
  }

  new_node->bitidx = bitidx;

  if(node->bitidx == 0 || getbit(key, node->bitidx))
    node->right = pat->count;
  else
    node->left = pat->count;

  ++pat->count;

  return pat->count - 2;
}

unsigned int patricia_lookup(const struct patricia* pat, const char* key)
{
  const struct pat_node* node = pat->nodes;
  const struct pat_node* next = pat->nodes + node->right;
  int maxbit = (strlen(key) + 1) * 8;

  while(node->bitidx < next->bitidx)
  {
    node = next;

    if(next->bitidx < maxbit && getbit(key, next->bitidx))
      next = pat->nodes + node->right;
    else
      next = pat->nodes + node->left;
  }

  return next - pat->nodes - 1;
}

unsigned int patricia_lookup_prefix(const struct patricia* pat, const char* prefix)
{
  const struct pat_node* node = pat->nodes;
  const struct pat_node* next = pat->nodes + node->right;
  int maxbit = (strlen(prefix) + 1) * 8;

  while(node->bitidx < next->bitidx)
  {
    node = next;

    if(next->bitidx < maxbit && getbit(prefix, next->bitidx))
      next = pat->nodes + node->right;
    else
      next = pat->nodes + node->left;
  }

  if(next == pat->nodes)
    return next - pat->nodes;

  return 0;
}

int patricia_write(const struct patricia* pat, int fd)
{
  size_t amount;

  if(sizeof(unsigned int) != write(fd, &pat->count, sizeof(unsigned int)))
    return -1;

  amount = pat->count * sizeof(struct pat_node);

  if(amount != write(fd, pat->nodes, amount))
    return -1;

  return sizeof(unsigned int) + amount;
}

void patricia_read(struct patricia* pat, int fd)
{
  unsigned int count;

  if(pat->mapped)
    pat->nodes = 0;

  if(read(fd, &count, sizeof(unsigned int)) < (ssize_t) sizeof(unsigned int))
    return;

  pat->count = count;

  pat->capacity = pat->count * sizeof(struct pat_node);
  pat->nodes = (struct pat_node*) realloc(pat->nodes, pat->capacity);
  read(fd, pat->nodes, pat->capacity);

  pat->mapped = 0;
}

size_t patricia_remap(struct patricia* pat, const void* data)
{
  const char* c;
  unsigned int count;

  if(!pat->mapped)
    free(pat->nodes);

  c = data;

  memcpy(&count, c, sizeof(unsigned int));
  c += sizeof(unsigned int);

  pat->count = count;
  pat->capacity = pat->count * sizeof(struct pat_node);
  pat->nodes = (struct pat_node*) c;
  c += pat->count * sizeof(struct pat_node);

  pat->mapped = 1;

  return c - (const char*) data;
}

void patricia_destroy(struct patricia* pat)
{
  if(!pat->mapped)
    free(pat->nodes);
  free(pat);
}

#ifndef NDEBUG
#include <stdio.h>

void patricia_print(struct patricia* pat, FILE* f)
{
  size_t i;

  fprintf(f, "digraph patricia {\n");
  fprintf(f, "rank = same;\n");
  fprintf(f, "0 [label=\"root\"]\n");

  for(i = 0; i < pat->count; ++i)
  {
    if(i && pat->get_key)
      fprintf(f, "%zu [label=\"%s,%u\"];\n", i, pat->get_key(i - 1, pat->arg), pat->nodes[i].bitidx - 1);

    fprintf(f, "%zu -> %u [label=\"0\"];\n", i, pat->nodes[i].left);
    fprintf(f, "%zu -> %u [label=\"1\"];\n", i, pat->nodes[i].right);
  }

  fprintf(f, "}\n");
}
#endif
