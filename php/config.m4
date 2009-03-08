dnl config.m4 for extension djpt

PHP_ARG_ENABLE(djptd, whether to enable djpt support,
               [  --enable-djpt         Enable djpt support])

if test "$PHP_DJPT" != "no"; then
  PHP_ADD_LIBRARY(djpt, 1, DJPT_SHARED_LIBADD)
  PHP_ADD_LIBRARY(jpt, 1, DJPT_SHARED_LIBADD)
  PHP_ADD_LIBRARY(gcrypt, 1, DJPT_SHARED_LIBADD)
  PHP_SUBST(DJPT_SHARED_LIBADD)
  PHP_NEW_EXTENSION(djpt, php-djpt.c, $ext_shared)
fi
