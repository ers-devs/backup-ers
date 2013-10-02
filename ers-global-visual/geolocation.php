<?php

// This code demonstrates how to lookup the country, region, city,
// postal code, latitude, and longitude by IP Address.
// It is designed to work with GeoIP/GeoLite City

// Note that you must download the New Format of GeoIP City (GEO-133).
// The old format (GEO-132) will not work.
define(PATH_GEO, 'geolocation/');

require PATH_GEO.'vendor/autoload.php';
include("geoipcity.inc");
include("geoipregionvars.php");

function getData($ip, $all) { 
   $gi = geoip_open(PATH_GEO."GeoLiteCity.dat",GEOIP_STANDARD);
   $record = geoip_record_by_addr($gi,$ip);
   $result = ""; 
   if( $all == "no" ) 
      $result =  $record->latitude . ' ' . $record->longitude . ' ' . $record->city . "\n";
   else {
      $result = $record->country_code . "," . $record->country_name . "," . $record->region . "," . $GEOIP_REGION_NAME[$record->country_code][$record->region];
      $result .= "," . $record->city . "," . $record->postal_code . "," . $record->metro_code . "," . $record->area_code . "," . $record->continent_code;
   }  
   geoip_close($gi);
   return $result;
}

$ip = $_GET['ip'];
if( isset($ip) && ! is_null($ip) )  {
   // pass this parameter if you want to get all info
   $all = $_GET['all'];
   if( isset($all) ) 
      $all = "yes"; 
   else 
      $all = "no";
   
   print getData($ip, $all); 
}
?>
