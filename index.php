<?php
$process = NULL;

try{
	require(getcwd() . "/" . "processor.php");	
	require(getcwd() . "/" . ".env");	
	$process = new Processor($env_config,"Yashi_2016\-05|Yashi_Advertisers","Yashi_Advertisers");
}catch(Error $e){
	print_r($e);
}



?>