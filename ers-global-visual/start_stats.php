<?php 

$graph = "demoNL";
shell_exec("python /home/murzo/ers/ers-local/ers/sync_global.py " . $graph . " > /dev/null 2>&1 & " );

$graph = "demoCH";
shell_exec("python /home/murzo/ers/ers-local/ers/sync_global.py " . $graph . " > /dev/null 2>&1 & " );
?>
