#ifndef PHP_DJPT_H_
#define PHP_DJPT_H_ 1

extern zend_module_entry djpt_module_entry;
#define phpext_djpt_ptr &djpt_module_entry

PHP_MINIT_FUNCTION(djpt);
PHP_MSHUTDOWN_FUNCTION(djpt);
PHP_RINIT_FUNCTION(djpt);
PHP_RSHUTDOWN_FUNCTION(djpt);
PHP_MINFO_FUNCTION(djpt);

PHP_FUNCTION(djpt_init);
PHP_FUNCTION(djpt_close);
PHP_FUNCTION(djpt_insert);
PHP_FUNCTION(djpt_append);
PHP_FUNCTION(djpt_replace);
PHP_FUNCTION(djpt_remove);
PHP_FUNCTION(djpt_has_key);
PHP_FUNCTION(djpt_get);
PHP_FUNCTION(djpt_column_scan);
PHP_FUNCTION(djpt_eval_string);
PHP_FUNCTION(djpt_get_counter);

#define DJPT_VERSION "1.0"

#ifdef ZTS
#define DJPT_G(v) TSRMG(djpt_globals_id, zend_djpt_globals *, v)
#else
#define DJPT_G(v) (djpt_globals.v)
#endif

#endif // !PHP_DJPT_H_
