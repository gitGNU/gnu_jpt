#include <ctype.h>
#include <errno.h>
#include <getopt.h>
#include <locale.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "jpt.h"
#include "jpt_internal.h"

static struct option long_options[] =
{
  { "help", 0, 0, 'h' },
  { "version", 0, 0, 'v' },
  { "binary", 0, 0, 'b' },
  { "replace", 0, 0, 'r' },
  { "append", 0, 0, 'a' },
  { "ignore", 0, 0, 'i' },
  { "mintime", 1, 0, 'm' },
  { 0, 0, 0, 0 }
};

static int recover = 0;
static int binary = 0;
static struct JPT_info* table;
static uint64_t mintime = 0;

static void
help(const char* argv0)
{
  printf("Usage: %s <TABLE-FILE> <COMMAND> [OPTION]...\n"
         "table front-end\n"
         "\n"
         "Mandatory arguments to long options are mandatory for short"
         " options too\n"
         "\n"
         "Commands:\n"
         "     backup <FILENAME> [COLUMN]\n"
         "                            backs up a table to a file\n"
         "     restore <FILENAME>     restores a table from a file\n"
         "     update                 reads row-column-value tuples and inserts them\n"
         "     insert ROW COLUMN      inserts a single value from standard input\n"
         "     lookup ROW COLUMN      searches for the given pattern\n"
         "     dump [COLUMN]          prints row-value pairs for an entire \n"
         "                            table or column\n"
         "     compact                performs a compaction\n"
         "     major-compact          performs a major compaction\n"
         "     recover                removes incomplete data\n"
         "     info                   print meta-information\n"
         "\n"
         "Options:\n"
         " -b, --binary               don't add trailing newline\n"
         " -r, --replace              replaces cell contents with new value\n"
         " -a, --append               appends new values to cell\n"
         " -i, --ignore               ignores new value if cell already has a value\n"
         " -m, --mintime=TIME         minimum time, for incremental backups\n"
         "     --help     display this help and exit\n"
         "     --version  display version information and exit\n"
         "\n"
         "If you specify several of -r, -a and -i, only the last option will be \n"
         "respected.  The default is -r.\n"
         "\n"
         "Report bugs to <morten@rashbox.org>.\n", argv0);
}

static void
init_table(const char* filename)
{
  table = jpt_init(filename, 128 * 1024 * 1024, recover ? JPT_RECOVER : 0);

  if(!table)
  {
    fprintf(stderr, "Failed to open table `%s': %s\n",
            filename, jpt_last_error());

    exit(EXIT_FAILURE);
  }
}

static int
data_callback(const char* row, const char* column, const void* data, size_t data_size, uint64_t* timestamp, void* arg)
{
  const char* cdata = data;

  printf("%s %s ", row, column);
  fwrite(data, 1, data_size, stdout);

  if(!binary && cdata[data_size - 1] != '\n')
    putchar('\n');

  return 0;
}

int
main(int argc, char** argv)
{
  int flags = JPT_REPLACE;

  setlocale(LC_ALL, "");

  for(;;)
  {
    int optindex = 0;
    int c;

    c = getopt_long(argc, argv, "braim:", long_options, &optindex);

    if(c == -1)
      break;

    switch(c)
    {
    case 'b':

      binary = 1;

      break;

    case 'r':

      flags = JPT_REPLACE;

      break;

    case 'a':

      flags = JPT_APPEND;

      break;

    case 'i':

      flags = 0;

      break;

    case 'm':

      mintime = strtoll(optarg, 0, 0);

      break;

    case 'h':

      help(argv[0]);

      return EXIT_SUCCESS;

    case 'v':

      printf(
        "jpt-control - Junoplay table administration utility\n"
        "Copyright (C) 2007 Morten Hustveit\n"
        "This is free software.  You may redistribute copies of it under the terms of\n"
        "the GNU General Public License <http://www.gnu.org/licenses/gpl.html>.\n"
        "There is NO WARRANTY, to the extent permitted by law.\n"
        "\n"
        "Written by Morten Hustveit.\n");

      return EXIT_SUCCESS;

    case '?':

      fprintf(stderr, "Try `%s --help' for more information.\n", argv[0]);

      return EXIT_FAILURE;
    }
  }

  if(optind + 1 >= argc)
  {
    fprintf(stderr, "Usage: %s <TABLE-FILE> <COMMAND> [OPTIONS]...\n", argv[0]);
    fprintf(stderr, "Try `%s --help' for more information.\n", argv[0]);

    return EXIT_FAILURE;
  }

  if(!strcmp(argv[optind + 1], "backup"))
  {
    if(optind + 3 != argc && optind + 4 != argc)
    {
      fprintf(stderr, "Usage: %s %s backup <FILENAME> [COLUMN]\n", argv[0], argv[optind]);
      fprintf(stderr, "Try `%s --help' for more information.\n", argv[0]);

      return EXIT_FAILURE;
    }

    init_table(argv[optind]);

    if(-1 == jpt_backup(table, argv[optind + 2], (optind + 4 == argc) ? argv[optind + 3] : 0, mintime))
    {
      fprintf(stderr, "Failed to backup to `%s': %s\n",
              argv[optind + 2], jpt_last_error());

      return EXIT_FAILURE;
    }
  }
  else if(!strcmp(argv[optind + 1], "restore"))
  {
    if(optind + 3 != argc)
    {
      fprintf(stderr, "Usage: %s %s restore <FILENAME>\n", argv[0], argv[optind]);
      fprintf(stderr, "Try `%s --help' for more information.\n", argv[0]);

      return EXIT_FAILURE;
    }

    init_table(argv[optind]);

    if(-1 == jpt_restore(table, argv[optind + 2], flags))
    {
      fprintf(stderr, "Failed to restore from `%s': %s\n",
              argv[optind + 2], jpt_last_error());

      return EXIT_FAILURE;
    }
  }
  else if(!strcmp(argv[optind + 1], "update"))
  {
    char line[4096];
    size_t lineno = 0;

    init_table(argv[optind]);

    while(fgets(line, sizeof(line), stdin))
    {
      char* row;
      char* row_end;
      char* column;
      char* column_end;
      char* value;
      char* value_end;

      ++lineno;

      row = line;

      while(isspace(*row))
        ++row;

      row_end = row;

      while(*row_end && !isspace(*row_end))
        ++row_end;

      if(!*row_end)
      {
        fprintf(stderr, "%zu: missing white-space after row name\n", lineno);

        continue;
      }

      *row_end = 0;
      column = row_end + 1;

      while(isspace(*column))
        ++column;

      column_end = column;

      while(*column_end && !isspace(*column_end))
        ++column_end;

      if(!*column_end)
      {
        fprintf(stderr, "%zu: missing white-space after column name\n", lineno);

        continue;
      }

      *column_end = 0;
      value = column_end + 1;

      while(isspace(*value))
        ++value;

      value_end = value;

      while(*value_end && *value_end != '\n' && value_end != (line + sizeof(line)))
        ++value_end;

      if(-1 == jpt_insert(table, row, column, value, value_end - value, flags))
        fprintf(stderr, "Failed to insert %zu bytes of data at %s/%s\n", value_end - value, row, column);
    }
  }
  else if(!strcmp(argv[optind + 1], "insert"))
  {
    char* value = 0;
    size_t value_size = 0;
    size_t value_alloc = 0;
    ssize_t res;

    if(optind + 4 != argc)
    {
      fprintf(stderr, "Usage: %s %s insert <ROW> <COLUMN>\n", argv[0], argv[optind]);
      fprintf(stderr, "Try `%s --help' for more information.\n", argv[0]);

      return EXIT_FAILURE;
    }

    init_table(argv[optind]);

    for(;;)
    {
      if(value_alloc == value_size)
      {
        value_alloc = value_alloc * 3 / 2 + 4096;
        value = realloc(value, value_alloc);
      }

      res = fread(value + value_size, 1, value_alloc - value_size, stdin);

      if(res == 0)
        break;

      if(res < 0)
      {
        perror("error reading from standard input");

        return EXIT_FAILURE;
      }

      value_size += res;
    }

    if(-1 == jpt_insert(table, argv[optind + 2], argv[optind + 3], value, value_size, flags))
    {
      perror("insert failed");

      return EXIT_FAILURE;
    }
  }
  else if(!strcmp(argv[optind + 1], "lookup"))
  {
    int result;
    void* value;
    size_t value_size;

    if(optind + 4 != argc)
    {
      fprintf(stderr, "Usage: %s %s lookup <ROW> <COLUMN>\n", argv[0], argv[optind]);
      fprintf(stderr, "Try `%s --help' for more information.\n", argv[0]);

      return EXIT_FAILURE;
    }

    init_table(argv[optind]);

    result = jpt_get(table, argv[optind + 2], argv[optind + 3], (void**) &value, &value_size);

    if(result == -1)
    {
      fprintf(stderr, "Could not find value at given cell: %s\n", jpt_last_error());

      return EXIT_FAILURE;
    }

    fwrite(value, 1, value_size, stdout);

    if(!binary && ((char*) value)[value_size - 1] != '\n')
      putchar('\n');
  }
  else if(!strcmp(argv[optind + 1], "dump"))
  {
    if(optind + 3 != argc && optind + 2 != argc)
    {
      fprintf(stderr, "Usage: %s %s dump [COLUMN]\n", argv[0], argv[optind]);
      fprintf(stderr, "Try `%s --help' for more information.\n", argv[0]);

      return EXIT_FAILURE;
    }

    init_table(argv[optind]);

    if(optind + 3 == argc)
      jpt_column_scan(table, argv[optind + 2], data_callback, 0);
    else
      jpt_scan(table, data_callback, 0);
  }
  else if(!strcmp(argv[optind + 1], "compact"))
  {
    init_table(argv[optind]);

    if(-1 == jpt_compact(table))
    {
      fprintf(stderr, "Compaction failed: %s\n", jpt_last_error());

      return EXIT_FAILURE;
    }
  }
  else if(!strcmp(argv[optind + 1], "major-compact"))
  {
    init_table(argv[optind]);

    if(-1 == jpt_major_compact(table))
    {
      fprintf(stderr, "Major compaction failed: %s\n", jpt_last_error());

      return EXIT_FAILURE;
    }
  }
  else if(!strcmp(argv[optind + 1], "recover"))
  {
    recover = 1;

    init_table(argv[optind]);
  }
  else if(!strcmp(argv[optind + 1], "info"))
  {
    init_table(argv[optind]);

    fprintf(stderr, "File size:       %'zu bytes\n", (size_t) table->file_size);
    fprintf(stderr, "Memory mapped:   %s\n", table->map_size ? "yes" : "no");
    fprintf(stderr, "Column count:    %'zu\n", table->column_count);
    fprintf(stderr, "Buffer size:     %'zu bytes\n", table->buffer_size);
    fprintf(stderr, "Disktable count: %'zu\n", table->disktable_count);
  }
  else
  {
    fprintf(stderr, "Unknown command `%s'.  Try `%s --help' for more information.\n", argv[optind + 1], argv[0]);

    return EXIT_FAILURE;
  }

  jpt_close(table);

  return EXIT_SUCCESS;
}
