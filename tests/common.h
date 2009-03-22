size_t test_count = 0;

#define WANT_POINTER(x) if( 0 == (x)) { fprintf(stderr, #x " failed unexpectedly: %s\n", jpt_last_error()); return EXIT_FAILURE; } ++test_count;
#define WANT_SUCCESS(x) if(-1 == (x)) { fprintf(stderr, #x " failed unexpectedly: %s\n", jpt_last_error()); return EXIT_FAILURE; } ++test_count;
#define WANT_FAILURE(x) if(-1 != (x)) { fprintf(stderr, #x " succeeded unexpectedly\n"); return EXIT_FAILURE; } ++test_count;
#define WANT_TRUE(x)    if(!(x)) { fprintf(stderr, #x " was false, expected true\n"); return EXIT_FAILURE; } ++test_count;
#define WANT_FALSE(x)   if((x)) { fprintf(stderr, #x " was true, expected false\n"); return EXIT_FAILURE; } ++test_count;
