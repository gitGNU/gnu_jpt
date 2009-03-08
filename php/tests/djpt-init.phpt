--TEST--
Testing djpt_init and djpt_close.
--FILE--
<?
$tempfile = tempnam('/tmp/', 'xphp-djpt-');

$tempdb = djpt_init($tempfile);
echo("$tempdb\n");
djpt_close($tempdb);

unlink($tempfile);
unlink($tempfile . '.log');
?>
--EXPECTF--
1
