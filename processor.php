<?php

//currently running this code from the browser
// it can be adjust to run from crontab
class Processor{ 
	
	private $path_current;
	private $config_file;
	private $data_files;
	private $data_local;
	private $data_local_current;
	private $advertiser_mapping;
	private $file_pattern;
	private $db_connection;
	private $skip_ftp;
	private $ftp;
	private $errorHandler;
	private $campaign_ids;
	private $order_ids;
	private $creative_ids;
	private $advertisers;
	private $records_processed;
	private $errorLogFile;
	private $purge_db;
	private $advertiser_mapping_file;
	
	//constructor
	public function Processor($config=NULL,$download_files_named=NULL,$advertiser_mapping=NULL){

		$this->errorLogFile = getcwd() . "/php_errors.log";
		ini_set("log_errors", 1);
		ini_set("error_log",$this->errorLogFile);

		//reset the log file - to reduce my scrolling
		$this->resetLog();

		//set up custom error handler for fun
		$this->errorHandler = function($errno, $errstr, $errfile, $errline){
			error_log($errline . "  " . $errstr . " " . $errfile);
		};
		set_error_handler($this->errorHandler);

		//testing settings
		$this->skip_ftp = false;
		$this->test_file = "yashi/Yashi_2016-05-01.csv";
		$this->purge_db = false;

		try{
			//check for config values
			if(!is_null($config) && isset($config["server"]) && isset($config["username"]) && isset($config["password"]) && isset($download_files_named) && isset($advertiser_mapping) ){
				$this->config_file = $config;

				//get local data directory to place files in 
				$this->data_local = getcwd() . "/data/";

				//checks if regular expression file pattern passed into constructor
				if(!is_null($download_files_named)){
					$this->file_pattern = "/" . $download_files_named . "/i";
				}
				else{
					//sets to anything
					$this->file_pattern = "/.*/";
				}

				if(!is_null($advertiser_mapping)){
					$this->advertiser_mapping_file = $advertiser_mapping;
					$this->advertiser_mapping = "/" . $advertiser_mapping . "/i";
				}

				if(!file_exists($this->data_local)) {
					mkdir($this->data_local,0766,false);
				}

				//holds records already processed to avoid duplicate entry messaging
				$this->records_processed = array();

				//create db object
				$this->mysqlConnection($this->config_file);

				//get campaign ids in DB is available
				$this->campaign_ids = $this->getTableIDs("yashi_campaign_id","campaign_id","zz__yashi_cgn");

				//get orders ids in DB is available
				$this->order_ids = $this->getTableIDs("yashi_order_id","order_id","zz__yashi_order");

				//get creative ids in DB is available
				$this->creative_ids = $this->getTableIDs("yashi_creative_id","creative_id","zz__yashi_creative");

				//truncate tables if set
				if($this->purge_db){
					$this->resetDatabase();	
				}

				//this was used for recursive file search but I am assuming all file put in data directory
				$this->data_local_current = NULL;

				//for testing skips ftp if set to true
				if(!$this->skip_ftp){
					$this->ftpConnection($config);	
				}
				else{
					echo "Processing";
					$this->cycleFiles();
					//$this->readCSVFile($this->test_file);
				}
				
			}
			else{
				error_log("Server Address, User Name, Password and Data File Are Required");
				throw new Exception("Server Address, User Name, Password and Data File Are Required");
			}	
		}catch(Exception $e){
			
		}
	}

	public function resetDatabase(){
		$this->db_connection->query("SET FOREIGN_KEY_CHECKS = 0");
		foreach( $this->config_file["db_tables"] as $table){
			$this->db_connection->query("truncate table " . $table);
		}
		$this->db_connection->query("SET FOREIGN_KEY_CHECKS = 1");
	}

	public function resetLog(){
		//https://stackoverflow.com/questions/4944388/emptying-a-file-with-php
		if (file_exists($this->errorLogFile)) {
		    $fp = fopen($this->errorLogFile , 'w'); 
		    fclose($fp);
		}
	}

	//create the associative array to use to get only certain pieces of data
	public function createAdvertisersObject($filename){
		try{
			if (($handle = fopen($this->data_local . $filename . ".csv", "r")) !== FALSE) {
			    while ($data = fgets($handle) ) {
			    	if(preg_match("/^[0-9]/",$data)){
			    		$line = explode(",",$data);
			    		$this->advertisers[$line[0]] = trim($line[1]);
			    	}
			    }
			    fclose($handle);
			}
		}
		catch(Exception $e){

		}
		
	}

	//gets ids of records inserted for data record db check
	public function getTableIDs($index,$value,$table){
		$result = $this->db_connection->query("select " . $index . "," . $value . " from " . $table);
		$ids = array();
		 while ($obj = $result->fetch_object()) {
		 	$ids[$obj->{$index}]=$obj->{$value};
    	}

    	return $ids;
	}

	//create database object
	public function mysqlConnection($config){
		$this->db_connection =  new mysqli($config["db_server"], $config["db_user"], $config["db_pass"], $config["db_name"]);
		try{
			if ($this->db_connection->connect_errno) {
		    	echo "Failed to connect to MySQL: (" . $this->db_connection->connect_errno . ") " . $this->db_connection->connect_error;
			}
		}catch(Error $e){

		}
	}

	//checks if resource is a diretcoyr or file 
	//was used for recursion but I decided that all files will be in a data directory
	public function checkIfFileOrDirectory($res){

		try{
			if(ftp_chdir($this->ftp->conn,$res)){
    			$this->retreiveFiles(ftp_nlist($this->ftp->conn, "."));
	    	}
	    	else{
	    		if(preg_match($this->file_pattern,$res)){
	    			$this->downloadFile($res);	
	    		}
	    	}
		}catch(Error $e){
			//echo $e->getMessage();
		}
	}

	//get a file listing
	public function retreiveFiles($location){
		foreach($location as $el){
		    $this->checkIfFileOrDirectory($el);
		}
		$this->data_local_current = NULL;
		ftp_cdup($this->ftp->conn);
	}

	//download the file
	public function downloadFile($filename){
		$filelocation = $this->data_local . $filename;

		if(!file_exists($filelocation)){
			if(ftp_get($this->ftp->conn,$filelocation, $filename ,FTP_ASCII)){
				error_log("Downloading to:" . $filelocation . "\n");
			}
			else{
				error_log("Failed download to: " . $filelocation . "\n");
			}
		}
		else{
			error_log("Already Downloaded: " . $filelocation);
		}
		
	}

	//selef explanatory
	public function ftpConnection($creds){
		//http://php.net/manual/en/ftp.examples-basic.php

		$this->ftp = new \stdClass;
		$this->ftp->conn = ftp_connect($creds["server"]); 
		$this->ftp->login = ftp_login($this->ftp->conn, $creds["username"], $creds["password"]); 
		if ((!$this->ftp->conn ) || (!$this->ftp->login)) { 
		    echo "FTP connection has failed!";
		    echo "Attempted to connect to " . $creds["server"] ." for user " . $creds["username"]; 
		    exit; 
		} else {
		    echo "Connected to " . $creds["server"] .", for user " . $creds["username"];
		    $this->retreiveFiles(ftp_nlist($this->ftp->conn, "."));

		   // $this->output($this->data_files);
		}

		// close the FTP stream 
		ftp_close($this->ftp->conn); 
		error_log("Finished FTP Transfer - Starting File Processing");
		$this->output("FTP Finished");
		$this->createAdvertisersObject($this->advertiser_mapping_file);
		$this->cycleFiles();
		
	}

	//the actual insert statment 
	public function dbInsertRequest($table="cgn",$insert_cols=NULL,$insert_vals=NULL){
		$query = "INSERT INTO " . $table . "(" . $insert_cols . ") VALUES(" . $insert_vals . ")";

		if(!($stmt = $this->db_connection->query($query) )){
			error_log($this->db_connection->errno . " " . $this->db_connection->error);
		}
	}

	//does a check for duplicate record if required and also returns the insert id which is used as a record id to prevent duplicate inserts
	public function insertData($table="cgn",$test_dup_query=NULL,$insert_cols=NULL,$insert_vals=NULL,$return_id=false){

		if(!is_null($test_dup_query)){
			$query = "select * from " . $table . " where " . $test_dup_query;
			$dup = $this->db_connection->query($query);

			if($dup->num_rows==0){
				$this->dbInsertRequest($table,$insert_cols,$insert_vals);
			}
		}else{
			$this->dbInsertRequest($table,$insert_cols,$insert_vals);
		}

		if($return_id){
			return  $this->db_connection->insert_id;
		}
	}

	//build the insert values from the passed object keys
	public function buildValueStringUsingKeys($obj=NULL,$keys=NULL){
		$finalString = "";
		$index = 0;
		foreach($keys as $k){
			if(isset($obj[$k])){
				$value = trim($obj[$k]);

				if(preg_match("/^date$/i",$k)){
					$value = $this->convertDateToTimeStamp($value);
				}
				$finalString .= '"' . $value . '"' . ((isset($keys[$index+1]))?",":"");
			}
			$index++;
		}
		return $finalString;
	}

	//self explanatory
	public function convertDateToTimeStamp($data){
		$text = explode("-",$data);
		return mktime(0, 0, 0,$text[1],$text[2],$text[0]);
	}

	//creates an assocative array key to prevent duplicate while processing
	public function makeDuplicateCheck($subset,$part1,$part2){
		$key_master = $part1 . "-" . $part2;
		$this->records_processed[$subset][$key_master] = true;
		return $part1 . "-" . $part2;
	}

	//checks for duplicate associative array key to prevent duplicate while processing
	public function checkForDuplicateRecord($subset,$part1,$part2){
		$key_master = $part1 . "-" . $part2;
		if(isset($this->records_processed[$subset][$key_master])){
			return true;
		}
		else{
			return false;
		}
	}

	//takes the linef romthe csv file and inserts the data into the database
	public function insertRecord($obj=NULL){

		//set timestampe for row
		$record_timestamp =  $this->convertDateToTimeStamp($obj["Date"]);
		//dupliacte object keys for data insert to DRY
		$general_data_keys = ["Date","Impressions","Clicks","25% Viewed","50% Viewed","75% Viewed","100% Viewed"];
		//duplicate columns for data insert to DRY
		$general_data_columns = "log_date,impression_count,click_count,25viewed_count,50viewed_count,75viewed_count,100viewed_count";
		
		//cgn
		//check if key is set in records_processed to help eliminate duplicate queries
		if(!$this->checkForDuplicateRecord("cgn",$obj["Campaign ID"],$obj["Advertiser ID"])){

			//builf insert values string
			$insertValues = $this->buildValueStringUsingKeys($obj,["Campaign ID","Campaign Name","Advertiser ID","Advertiser Name"]);

			//used to check if a matching record is alredy in DB
			$checkQuery = "yashi_campaign_id=" . $obj["Campaign ID"] . " and yashi_advertiser_id=" . $obj["Advertiser ID"];

			//retrieve last insert id, if not already existing in DB, to create the campaign id to be used in cgn_data and order query
			$campaign_id = $this->insertData($this->config_file["db_tables"]["cgn"],$checkQuery,"yashi_campaign_id,name,yashi_advertiser_id,advertiser_name",$insertValues,true);	
		
			// if the insert id is not zero add to associative array
			//I am tryikngto limit queries to speed up script
			if($campaign_id!=0){
				$this->campaign_ids[$obj["Campaign ID"]] = $campaign_id;	
			}

			//add check key in records_processed object to eliminate duplicate queries to speed up script
			$this->makeDuplicateCheck("cgn",$obj["Campaign ID"],$obj["Advertiser ID"]);
			
		}

		//cgn_data
		//similar process as above but different insert and column values
		if(!$this->checkForDuplicateRecord("cgn_data",$this->campaign_ids[$obj["Campaign ID"]],$record_timestamp) ){
			$insertValues = $this->campaign_ids[$obj["Campaign ID"]] . ",";
			$insertValues .= $this->buildValueStringUsingKeys($obj,$general_data_keys);
			$insertColumns = "campaign_id," . $general_data_columns;
			$this->insertData($this->config_file["db_tables"]["cgn_data"],NULL,$insertColumns,$insertValues);
			$this->makeDuplicateCheck("cgn_data",$this->campaign_ids[$obj["Campaign ID"]],$record_timestamp);
		}

		// //order
		//similar process as above but different insert and column values
		if(!$this->checkForDuplicateRecord("order",$this->campaign_ids[$obj["Campaign ID"]],$obj["Order ID"]) ){
			$insertValues = $this->campaign_ids[$obj["Campaign ID"]] . ",";
			$insertValues .= $this->buildValueStringUsingKeys($obj,["Order ID","Order Name"]);
			$checkQuery = "campaign_id=" . $this->campaign_ids[$obj["Campaign ID"]] . " and yashi_order_id=" . $obj["Order ID"];
			//$checkQuery = NULL;
			$order_id = $this->insertData($this->config_file["db_tables"]["order"],$checkQuery,"campaign_id,yashi_order_id,name",$insertValues,true);
			if($order_id!=0){
				$this->order_ids[$obj["Order ID"]] = $order_id;	
			}
			$this->makeDuplicateCheck("order",$this->campaign_ids[$obj["Campaign ID"]],$obj["Order ID"]);
		}

		//order_data
		//similar process as above but different insert and column values
		if(!$this->checkForDuplicateRecord("order_data",$this->order_ids[$obj["Order ID"]],$record_timestamp) ){
			$insertValues = $this->order_ids[$obj["Order ID"]] . ",";
			$insertValues .= $this->buildValueStringUsingKeys($obj,$general_data_keys);
			$insertColumns = "order_id," . $general_data_columns;
			$this->insertData($this->config_file["db_tables"]["order_data"],NULL,$insertColumns,$insertValues);
			$this->makeDuplicateCheck("order_data",$this->order_ids[$obj["Order ID"]],$record_timestamp);
		}

		//creat
		//similar process as above but different insert and column values
		if(!$this->checkForDuplicateRecord("creat",$this->order_ids[$obj["Order ID"]],$obj["Creative ID"]) ){
			$insertValues = $this->order_ids[$obj["Order ID"]] . ",";
			$insertValues .= $this->buildValueStringUsingKeys($obj,["Creative ID","Creative Name","Creative Preview URL"]);
			$checkQuery = "order_id=" . $this->order_ids[$obj["Order ID"]] . " and yashi_creative_id=" . $obj["Creative ID"];
			$creative_id = $this->insertData($this->config_file["db_tables"]["creat"],$checkQuery,"order_id,yashi_creative_id,name,preview_url",$insertValues,true);
			if($creative_id!=0){
				$this->creative_ids[$obj["Creative ID"]] = $creative_id;	
			}
			$this->makeDuplicateCheck("creat",$this->order_ids[$obj["Order ID"]],$obj["Creative ID"]);
		}

		//creat_data
		//similar process as above but different insert and column values
		if(!$this->checkForDuplicateRecord("creat_data",$this->creative_ids[$obj["Creative ID"]],$record_timestamp) ){
			$insertValues = $this->creative_ids[$obj["Creative ID"]] . ",";
			$insertValues .= $this->buildValueStringUsingKeys($obj,$general_data_keys);
			$insertColumns = "creative_id," . $general_data_columns;
			$this->insertData($this->config_file["db_tables"]["creat_data"],NULL,$insertColumns,$insertValues);	
			$this->makeDuplicateCheck("creat_data",$this->creative_ids[$obj["Creative ID"]],$record_timestamp);
		}		
	}

	//starts the data processing per file after the ftp download is completed
	public function cycleFiles(){

		foreach (glob($this->data_local ."*") as $filename) {

			$process_file = true;
			if(isset($this->advertiser_mapping)){
				$process_file = !preg_match($this->advertiser_mapping,$filename);	
			}

			if($process_file){
			 	$this->readCSVFile($filename);
			}
		}
	}

	//parses the data in the passed filename 
	public function readCSVFile($filename){
		//https://stackoverflow.com/questions/9139202/how-to-parse-a-csv-file-using-php
		$array_keys = [];
		$data_object = [];
		$data_indice = 0;
		$sub_data_object = [];
		$row = 0;
		if (($handle = fopen($filename, "r")) !== FALSE) {
		    while (($data = fgetcsv($handle, 1000, ",")) !== FALSE) {
		        $num = count($data);
		        //goes line by line and build data object
		        for ($c=0; $c < $num; $c++) {
		        	if($row==0){
			    		$array_keys[] = $data[$c];
			    	}
			    	else{
			    		if($num==count($array_keys)){
			    			$sub_data_object[$data_indice][((String)$array_keys[$c])] = $data[$c];
			    		}
			    	}
		        }

		        //if the object size matchs the key count continue
		        if($row>0 && sizeof($sub_data_object[$data_indice])==$num ){
		        	//check if advertiser in line

		        	//if the object contain an advertiser id passed in the Mapping file then porceeds to insert
		        	if(isset($this->advertisers[trim($sub_data_object[$data_indice]["Advertiser ID"])] ) ){
		        		$adname = $this->advertisers[trim($sub_data_object[$data_indice]["Advertiser ID"] ) ];
		        		$sub_data_object[$data_indice]["Advertiser Name"] = $adname;
		        		$this->output($sub_data_object[$data_indice]); //just output to see something in browser
		        		$this->insertRecord($sub_data_object[$data_indice]);	
		        	}
		        }
		        
		        $row++;
		        $data_indice++;
		    }

		    fclose($handle);
		}
	}

	//use this to run from browser
	public function output($input){
		echo "<pre>";
		var_dump($input);
		echo "</pre>";
	}

};