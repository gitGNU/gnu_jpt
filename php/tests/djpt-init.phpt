--TEST--
Testing djpt_init and djpt_close.
--FILE--
<?
$tempfile = tempnam('/tmp/', 'php-djpt-');

$tempdb = djpt_init($tempfile);
djpt_close($tempdb);

unlink($tempfile);
?>
--EXPECTF--
