/*  I/O functions for jpt.
    Copyright (C) 2007-2008  Morten Hustveit <morten@rashbox.org>

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

#include <stdio.h>

#include "jpt_internal.h"

int
JPT_write_uint(FILE* f, unsigned int integer)
{
  if(integer > 0xfffffff)
  {
    if(EOF == fputc(0x80 | ((integer >> 28) & 0x7f), f))
      return -1;
  }

  if(integer > 0x1fffff)
  {
    if(EOF == fputc(0x80 | ((integer >> 21) & 0x7f), f))
      return -1;
  }

  if(integer > 0x3fff)
  {
    if(EOF == fputc(0x80 | ((integer >> 14) & 0x7f), f))
      return -1;
  }

  if(integer > 0x7f)
  {
    if(EOF == fputc(0x80 | ((integer >> 7) & 0x7f), f))
      return -1;
  }

  if(EOF == fputc(integer & 0x7f, f))
    return -1;

  return 0;
}

int
JPT_write_uint64(FILE* f, uint64_t val)
{
  int i;

  for(i = 0; i < 8; ++i)
  {
    if(EOF == fputc(val >> ((7 - i) * 8), f))
      return -1;
  }

  return 0;
}

uint64_t
JPT_read_uint(FILE* f)
{
  uint64_t result = 0;
  int c;

  while(EOF != (c = fgetc(f)))
  {
    result <<= 7;
    result |= (c & 0x7f);

    if(!(c & 0x80))
      break;
  }

  return result;
}

uint64_t
JPT_read_uint64(FILE* f)
{
  uint64_t result = 0;
  int i;

  for(i = 0; i < 8; ++i)
  {
    result <<= 8ULL;
    result |= (uint64_t) fgetc(f);
  }

  return result;
}
