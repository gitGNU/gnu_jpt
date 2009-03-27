#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/signal.h>
#include <sys/mman.h>
#include <unistd.h>

#include "jpt.h"

#include "jpt_internal.h"

const char* tokens[] =
{
  "SPJUGWKLMMFRPIQ", "CGZRQPGP", "FPK", "JMPBDPPKUJYGSLVIMHPKYFOYPTXJVD",
  "TRBWCTJFNCBCQ", "WKIBIYHDFTDTKOD", "UOHYYIBDZWXQZLZZKVLLW", "VYCPDP",
  "BFQTUVNSVBURTRINKZUG", "JVZ", "FVSMRZ", "XQWPPSUQGYDZSVGMLWUPTVMKHLPCU",
  "BKGPUXNUCSXVROBFZZJDSBVNFJQM", "IDLZRVRHZRP", "KYI", "NVIWXCTXKKIMLL",
  "CMPDOPVFDWUJOQV", "KQT", "WBMV", "SLXVQKFQ", "JXUJDFUWLNUCDV", "JIMJJTGRF",
  "ZVJG", "DKZIWKMFUZMJDZPV", "PDMUQ", "DBPRLTWLTVKYTMWOIMXGUMIWFRYPGJHXCHSMDWN",
  "FDXQVVF", "XYLRTJMTFSVMOKTCYNBZOZMJZN",
  "LZNWUSWPSLQDOYUUOHQFUQKDJBQYTBSCGVRRNHJUBQ", "VVQPYLKPO", "IXS",
  "ONIVXNPDWKPYX", "VID", "DJLZFKFGHZ", "UGJLBY", "FQZXQFKTQJOXTDDCQYC",
  "OLYOKB", "ZJTLBWOMTCVFJD", "FCC",
  "XUPQUQPVTXCOTPOYDCRKRUQWGGGCMTIRNFHLHLSZFJ",
  "BCJFLNPYNBNLGPBVODONRSVQMWVBKXNFTJQYUNMNPU", "RDUZQRT", "FHULHD",
  "OXPHRFLKWTWSKU", "PGR", "PLTHZUVUUVBHIXPCMOMJHIMVWFUN", "YDJ", "HBKQBJMZDWR",
  "FWGMDQHRMNHXDXRYZYKIPH", "BIBORH", "CITNS", "CJUWMKLMOMFVHQF", "PHCTQS",
  "MIOTBYHPCIMPIT", "KKKS", "KDMNU", "MPXSBVDUYR", "ILJ", "IXWC",
  "OPTTDOSWXSYUIYUI", "OXXTZPCBDBYJKP", "KXFYPMJRVTMGYTTQYBLVSCDLZT", "KVQSF",
  "QYTPS", "WTIVYVPHHMKNB", "HBHKNTLLN", "BTTJNFFWUXRKHYJJPNFLMBSJRSZL",
  "BBQVHKBNTQLGGOWGGTVT", "RLVQJ", "ODJNCYWVOZXDVZZTDBZ", "PSCJWWBFUOVDZOFU",
  "TMDRNQ", "RCFVZMGRCRLOY", "CSBRNFYH", "QOQ", "WOYRS", "PFI",
  "OILJIDRHPKVGCKPCMHKDGT", "ZQRZKYTSNSIUYQSMOHIBCSD", "HNNNWIY",
  "QXLRNHOGPIOCRWJSVGJQFOGHVYGPIFMQXQVQSUICUDTVKZLRPBYYKSJ", "CIWCMZLH", "OOS",
  "IRBUVOHBHFFRVDWXLFNNYYXHY", "RIRCUB", "WNOT", "NINGFHZKSNJQOMHJZZSUCFN",
  "WRUIHXOWMTWTQZUGKR", "GOYRMGXHUTMCUOVTOGMGVWJPCSMQRGWDWXRNGIIY", "DUTQFJF",
  "OLZZDJPQR", "XTHOW", "DSPBF", "PZOGXUXY", "LBDNWUSXCTNRZKN", "BPOIZKZSHKF",
  "YOV", "DK", "TUISJKKNJZLYDXIYWWGCJRRHB", "SNC",
  "RNMXOXUJBWZSSRQIZBPFUDDRTDXOWDRWHOMZTU", "HQW", "WOS",
  "XXIDBRVPTJYPUKFKVCTQY", "MFXKDBSQMOXZBCBBLQHRCWD", "YCMJDITF", "NZYODG",
  "FZQLV", "BROZYJXFGTFN", "IIQCTBSTNLTZHRHUMDMNCRQCUJJMOMWNLBSNP", "OYROM",
  "NLTWPKBBPOFRSLVLOXJ", "BCFWJBKVUVUHGYHKRZTBUBRJIMW",
  "XBCTGFQSHLRDQHLMGLKUWQMJJKTCOCVWVMKNWXTLI",
  "GBCKOSIKJVXQOJTVBVJCRGYCXYORKNZHYINRLHBG", "SNQTYBYQ", "FKQURXVHYIUH",
  "RRQZKYW", "MDX", "TFTLVLC", "PXUYBZUCSULTKOCTKSWUTNDZ", "TQJUDYG", "KFNZD",
  "HHFYBJCQPHTFRQJFUOVHOUYMV", "TCLILJFRWX", "ZBWZWMJSHMOUXLU",
  "ZXDIUPFLRVDJNKGQM", "RTFWBVB", "JVLUHQUYIG", "MKTXUWGIQZSGKCSUSRZMIWF",
  "RGWOUHCYOMZWUWMOXTKFWHOTTGDOMLXMJRHUPRGYHQH", "TVLNPVZTYLRHCJRYIYTIYCMUGBK",
  "UMPK", "HHLYYN", "QHMBYHJLVXOOSOFG", "PNKRJVZTCSJTQ", "JLDJMRDSKX", "ZQGKX",
  "DFVOQUMGWPGTMQTZS", "HCWJVFPJHLHBPNWLWVBDRPVYRM", "FQPQCHI", "MC", "HIDBNNDK",
  "XLNPQ", "RUG", "PUISWH", "LDQIQTTWIPIKHYQ", "CHFJKL",
  "UIMLIXQJRJVIJMXUDRIICMFJJXNOQLZTNJUU", "SOFZWVMCVJBIXSXTFNLGYV",
  "FVKUUGJIFMSOJBBRQHJJHHHHYQCWRNHG", "DQXSKHIXJZVUPSQF", "DRPIZHYIHMTUIZDK",
  "GSFMNJPYTWITNYNLJKLMN", "OINSIQVBXKH",
  "VQZXFKNTDVQUPWOBXPFNIZPFJBOHGSDPUDDVQPPFBTZM",
  "GBIYLNYHSUQFQBICMVBCWPMDRTRQMCOXJSXBMPIHQQZMQFWXJKBUMGLZZK",
  "STKDZSRPTXXOUIMIVSHUQOCKQX", "GUUFRNVL", "VPRNPDVU", "OBULMJZUNMRWD",
  "BDYPCUYUSGLSUYK", "ZVHSHRDO", "JRQSHPUOIB", "NFGXOSIUPVKBZLZUM",
  "NLVTKUVWMTSUKIQDIUNF", "BIFVMOMMSIDZBXIPRMMJNNPOHVRNRMLL", "KVQKMTQGLKQNIOJR",
  "BLVSJCOUOPVDBIDZHOBSQVCMZGVYWMTVSXSNX", "GBQRTL", "IUUGT", "QV",
  "RITTMCUBXZBHVLJIDRDFGHHD", "ULPVXN", "CXYTJOJRTKLNIK",
  "TDCGTVCPKKBGKQYDBIPZTLORJUZFSSOOYRYBSD", "DPLCFMHUTWNR",
  "JGBGHXPTHNGZSGSKFSJMNBZIYUTHDVTLNNOGMGLBBH", "ISS",
  "SGSNNIZFVUBQKPXKLCPHHOZRTIH", "JOHT", "XYTGSCJTIWCD", "TLKSOCOWGFIVZWL",
  "RTWBLJOCVDGUCDJFKQV", "FMIGYUVXVXNPTWZQNZS", "UKNQIBOWXIVCIYU",
  "RLZXTYQXYKFPIVFXQXJVRYYSBFSYIGMVDCIZTQXRCXSDJMSWUCBXNPBOBDYIDODOUDYCVJPHHUQHWMQPRITD",
  "GJGLYVO", "VXT", "WWMJPPLDRP", "UZQXQFCFXHNPOI", "KMSBGSHKTCU",
  "TQVGHHHYOQIVNQVOPDDWJPHNNZOVDBKPH", "GPCCQBXRLVGSJJLLOXXHPBMNXJ",
  "SQTJLRJDZWWRTNRXNQQ", "HLSYDLGKQPFBUHONV", "VPOOMJ", "YVPDMZXKS",
  "XTINKJIQLIBBCVLVJXNKYRUVSVKFTUKYU", "ZRNCYUFV", "JMVLLVNCTMFXRTZKKXFQ",
  "DFJGVHW", "LWJOIF", "KXCHLBWVKQWUKYXYJQIFXP", "MJ", "MVWBIRRJNTGJHWJTKUUKRCP",
  "YUUOGSRQRJCOW", "QUT", "FRILSRWWKQKLL", "VKTOU", "KWSN", "CHGVUVKWBUXRWDMJH",
  "QRMQQJTLGYDZFZXIBGDV", "MPLSUHMFTSNTWRQFLJYK", "SUPWNMIFGKFSJHHF",
  "IXSSIFPWSSLSRZL", "BLN", "KLJKMJGTJ", "XKLPZV",
  "TGWYUIFMZQVWILQTZRFLRNWRUVQOJVDYINZSOTDHCIHZTXWSFQSFNYLMVGPJGH", "NODNLUM",
  "JTRLTWPQLYB", "MCYPHG", "JJNCZR", "UZLGOYV",
  "OHXZRDUBNGPFYMXPJFFBLKRWJZMKBSUVBWQJTTVTZUQYTLMLNUCBJPVSWUHMLK",
  "HRPLJVOQVDBJLJJMW", "NTGB", "RVS", "WDBFYRKVUKRJP", "PNIVXPGHQF",
  "GMXUGVIMWZQWCDNQGDUOBTO", "DICL", "HKVJCL", "SDPDNSWDYXNDDZZTR", "VXRGL",
  "MOWL", "ZXOXUKU", "QRTNKKZ", "NUMSXVRZ", "VSIKRLYBVLTIBRIIHBI", "UWL", "FBQW",
  "HYFG", "LDKNPSOYTJRVOWZNYZOV", "ZZVQLW", "DXJJGMD", "ZOMKJZJGQXZTUY",
  "OXFJIZIV", "KF", "ZQDIKDQNKHWNQU", "WYKMWNF", "WLXZZXJPIHKFVGPOKH",
  "JCOWXNSK", "UYIPZURKYQZBKTYUGPTFSQOQW",
};

size_t test_count = 0;

#define TOKEN_COUNT (sizeof(tokens) / sizeof(tokens[0]))

#define WANT_POINTER(x) do { if( 0 == (x)) { fprintf(stderr, "\nTest %zu\n" #x " at line %d failed unexpectedly: %s\n", test_count, __LINE__, jpt_last_error()); abort(); } pthread_mutex_lock(&counter_lock); ++test_count; pthread_mutex_unlock(&counter_lock); } while(0)
#define WANT_SUCCESS(x) do { if(-1 == (x)) { fprintf(stderr, "\nTest %zu\n" #x " at line %d failed unexpectedly: %s\n", test_count, __LINE__, jpt_last_error()); abort(); } pthread_mutex_lock(&counter_lock); ++test_count; pthread_mutex_unlock(&counter_lock); } while(0)
#define WANT_FAILURE(x) do { if(-1 != (x)) { fprintf(stderr, "\nTest %zu\n" #x " at line %d succeeded unexpectedly\n", test_count, __LINE__); abort(); } pthread_mutex_lock(&counter_lock); ++test_count; pthread_mutex_unlock(&counter_lock); } while(0)
#define WANT_TRUE(x)    do { if(!(x)) { fprintf(stderr, "\nTest %zu\n" #x " at line %d was false, expected true\n", test_count, __LINE__); abort(); } pthread_mutex_lock(&counter_lock); ++test_count; pthread_mutex_unlock(&counter_lock); } while(0)
#define WANT_FALSE(x)   do { if((x)) { fprintf(stderr, "\nTest %zu\n" #x " at line %d was true, expected false\n", test_count, __LINE__); abort(); } pthread_mutex_lock(&counter_lock); ++test_count; pthread_mutex_unlock(&counter_lock); } while(0)

struct value
{
  char* data;
  size_t size;
};

static struct value values[TOKEN_COUNT][TOKEN_COUNT];

static pthread_mutex_t row_locks[TOKEN_COUNT];
static pthread_mutex_t col_locks[TOKEN_COUNT];
static pthread_mutex_t counter_lock = PTHREAD_MUTEX_INITIALIZER;

static struct JPT_info* db;

static pthread_t threads[2];

static int done;

static void
sighandler(int signal)
{
  if(done)
    exit(EXIT_SUCCESS);

  done = 1;
}

static void
exithandler()
{
  int i, j;

  for(i = 0; i < TOKEN_COUNT; ++i)
    for(j = 0; j < TOKEN_COUNT; ++j)
      free(values[i][j].data);

  jpt_close(db);

  unlink("test-db.tab");
  unlink("test-db.tab.log");
}

static int
cell_callback(const char* row, const char* column, const void* data, size_t data_size, uint64_t* timestamp, void* arg)
{
  size_t* count = arg;
  size_t nrow, ncolumn;
  ++*count;

  for(nrow = 0; nrow < TOKEN_COUNT; ++nrow)
    if(!strcmp(tokens[nrow], row))
      break;

  for(ncolumn = 0; ncolumn < TOKEN_COUNT; ++ncolumn)
    if(!strcmp(tokens[ncolumn], column))
      break;

  if(data_size > values[nrow][ncolumn].size)
  {
    fprintf(stderr, "\"%s\", \"%s\": %d bytes in database, wanted %d\n", row, column, (int) data_size, (int) values[nrow][ncolumn].size);
    abort();
  }

  if(memcmp(values[nrow][ncolumn].data, data, data_size))
  {
    size_t i;

    fprintf(stderr, "Data mismatch at \"%s\", \"%s\":\n", row, column);
    fprintf(stderr, "Wanted: ");

    for(i = 0; i < data_size; ++i)
      fprintf(stderr, "%c", values[nrow][ncolumn].data[i]);

    fprintf(stderr, "\n");
    fprintf(stderr, "Got:    ");

    for(i = 0; i < data_size; ++i)
      fprintf(stderr, "%c", ((char*) data)[i]);

    fprintf(stderr, "\n");

    abort();
  }

  return 0;
}

static void*
test_thread(void* arg)
{
  void* ret;
  size_t retsize;
  unsigned int seed = 0;

  while(!done)
  {
    struct value* v;
    int action = rand_r(&seed) % 6;
    int row = rand_r(&seed) % TOKEN_COUNT;
    int col = rand_r(&seed) % TOKEN_COUNT;
    int token = rand_r(&seed) % TOKEN_COUNT;
    size_t tokenlen = strlen(tokens[token]);
    size_t i, j;

    pthread_mutex_lock(&row_locks[row]);
    pthread_mutex_lock(&col_locks[col]);

    v = &values[row][col];

    switch(action)
    {
    case 0:

      {
        free(v->data);

        v->size = tokenlen;
        v->data = malloc(v->size);
        memcpy(v->data, tokens[token], tokenlen);

        WANT_SUCCESS(jpt_insert(db, tokens[row], tokens[col], tokens[token], tokenlen, JPT_REPLACE));

        WANT_SUCCESS(jpt_get(db, tokens[row], tokens[col], &ret, &retsize));
        WANT_TRUE(retsize == tokenlen);
        WANT_TRUE(!memcmp(ret, tokens[token], tokenlen));
        free(ret);
      }

      break;

    case 1:

      {
        if(v->data)
        {
          WANT_SUCCESS(jpt_get(db, tokens[row], tokens[col], &ret, &retsize));
          WANT_TRUE(retsize == v->size);
          WANT_TRUE(!memcmp(ret, v->data, retsize));
          free(ret);
        }
        else
        {
          WANT_FAILURE(jpt_get(db, tokens[row], tokens[col], &ret, &retsize));
        }
      }

      break;

    case 2:

      {
        if(v->data)
        {
          WANT_SUCCESS(jpt_remove(db, tokens[row], tokens[col]));
        }
        else
        {
          WANT_FAILURE(jpt_remove(db, tokens[row], tokens[col]));
          WANT_TRUE(errno == ENOENT);
        }

        free(v->data);
        v->data = 0;
        v->size = 0;
      }

      break;

    case 3:

      {
        v->data = realloc(v->data, v->size + tokenlen);
        memcpy(v->data + v->size, tokens[token], tokenlen);
        v->size += tokenlen;

        WANT_SUCCESS(jpt_insert(db, tokens[row], tokens[col], tokens[token], tokenlen, JPT_APPEND));
      }

      break;

    case 4:

      {
        if(v->data)
        {
          int has_key = jpt_has_key(db, tokens[row], tokens[col]);

          if(0 == jpt_insert(db, tokens[row], tokens[col], tokens[token], tokenlen, 0))
          {
            fprintf(stderr, "jpt_insert of \"%s\", \"%s\" succeeded unexpectedly.  Has key: %d\n",
                    tokens[row], tokens[col], has_key);

            exit(EXIT_FAILURE);
          }
        }
        else
        {
          v->size = tokenlen;
          v->data = malloc(v->size);
          memcpy(v->data, tokens[token], tokenlen);

          WANT_SUCCESS(jpt_insert(db, tokens[row], tokens[col], tokens[token], tokenlen, 0));
        }
      }

      break;

    case 5:

      {
        size_t count = 0;
        size_t vfy_count = 0;

        for(i = 0; i < TOKEN_COUNT; ++i)
          if(values[i][col].data)
            ++vfy_count;

        if(vfy_count)
        {
          WANT_SUCCESS(jpt_column_scan(db, tokens[col], cell_callback, &count));
          WANT_TRUE(count == vfy_count);
        }
        else
        {
          WANT_TRUE(0 == jpt_column_scan(db, tokens[col], cell_callback, &count) || errno == ENOENT);
          WANT_TRUE(count == vfy_count);
        }
      }

      break;
    }

    if((rand_r(&seed) % 100) == 0)
    {
      WANT_SUCCESS(jpt_remove_column(db, tokens[col], 0));
      WANT_FAILURE(jpt_has_column(db, tokens[col]));

      for(i = 0; i < TOKEN_COUNT; ++i)
      {
        free(values[i][col].data);
        values[i][col].data = 0;
        values[i][col].size = 0;
      }
    }
    else if((rand_r(&seed) % 3000) == 0)
    {
      WANT_SUCCESS(jpt_compact(db));
    }
    else if((rand_r(&seed) % 30000) == 0)
    {
      WANT_SUCCESS(jpt_major_compact(db));
    }

    pthread_mutex_unlock(&col_locks[col]);
    pthread_mutex_unlock(&row_locks[row]);

    if((rand_r(&seed) % 1000) == 0)
    {
      size_t count = 0;
      size_t vfy_count = 0;

      for(i = 0; i < TOKEN_COUNT; ++i)
        pthread_mutex_lock(&row_locks[i]);

      for(i = 0; i < TOKEN_COUNT; ++i)
        pthread_mutex_lock(&col_locks[i]);

      for(i = 0; i < TOKEN_COUNT; ++i)
        for(j = 0; j < TOKEN_COUNT; ++j)
          if(values[i][j].data)
            ++vfy_count;

      if(vfy_count)
      {
        WANT_SUCCESS(jpt_scan(db, cell_callback, &count));
        WANT_TRUE(count == vfy_count);
      }
      else
      {
        WANT_TRUE(0 == jpt_scan(db, cell_callback, &count) || errno == ENOENT);
        WANT_TRUE(count == vfy_count);
      }

      for(i = 0; i < TOKEN_COUNT; ++i)
        pthread_mutex_unlock(&col_locks[i]);

      for(i = 0; i < TOKEN_COUNT; ++i)
        pthread_mutex_unlock(&row_locks[i]);
    }

    /* Close and reopen */
    if((rand_r(&seed) % 10000) == 0)
    {
      for(i = 0; i < TOKEN_COUNT; ++i)
        pthread_mutex_lock(&row_locks[i]);

      for(i = 0; i < TOKEN_COUNT; ++i)
        pthread_mutex_lock(&col_locks[i]);

      jpt_close(db);
      WANT_POINTER(db = jpt_init("test-db.tab", 128 * 1024, 0));

      for(i = 0; i < TOKEN_COUNT; ++i)
        pthread_mutex_unlock(&col_locks[i]);

      for(i = 0; i < TOKEN_COUNT; ++i)
        pthread_mutex_unlock(&row_locks[i]);
    }
  }

  return 0;
}

int
main(int argc, char** argv)
{
  size_t i;

  WANT_TRUE(0 == unlink("test-db.tab") || errno == ENOENT);
  WANT_TRUE(0 == unlink("test-db.tab.log") || errno == ENOENT);

  WANT_POINTER(db = jpt_init("test-db.tab", 128 * 1024, 0));

  if(!db)
    return EXIT_FAILURE;

  signal(SIGINT, sighandler);
  atexit(exithandler);

  printf("0 tests ok ");
  fflush(stdout);

  for(i = 0; i < TOKEN_COUNT; ++i)
  {
    pthread_mutex_init(&row_locks[i], 0);
    pthread_mutex_init(&col_locks[i], 0);
  }

  for(i = 0; i < sizeof(threads) / sizeof(threads[0]); ++i)
    pthread_create(&threads[i], 0, test_thread, 0);

  while(!done)
  {
    volatile size_t n;

    usleep(200000);

    pthread_mutex_lock(&counter_lock);
    n = test_count;
    pthread_mutex_unlock(&counter_lock);

    printf("\r%zu tests ok ", n);
    fflush(stdout);
  }

  for(i = 0; i < sizeof(threads) / sizeof(threads[0]); ++i)
    pthread_join(threads[i], 0);

  return EXIT_SUCCESS;
}
