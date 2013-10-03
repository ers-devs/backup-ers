var map;

var SERVER_IP="82.196.15.149";
var ERS_SERVER_PORT="8080";
var ERS_GEO_PORT="80";
var ERS_PATH="ers";

function convertTimestampToDate(unix_timestamp) { 
   // create a new javascript Date object based on the timestamp
   // multiplied by 1000 so that the argument is in milliseconds, not seconds
   var date = new Date(unix_timestamp*1);

   var day = date.getUTCDate(); 
   var month = date.getUTCMonth(); 
   var year = date.getUTCFullYear();
   
   // hours part from the timestamp
   var hours = date.getUTCHours();
   // minutes part from the timestamp
   var minutes = date.getUTCMinutes();
   // seconds part from the timestamp
   var seconds = date.getUTCSeconds();

   // will display time in 10:30:23 format
   var formattedTime = day + '.'  + month + '.' + year + ' - ' + hours + ':' + minutes + ':' + seconds;
   return formattedTime;
}

function httpGet(theUrl) {
   var xmlHttp = null;
   xmlHttp = new XMLHttpRequest();
   xmlHttp.open( "GET", theUrl, false );
   xmlHttp.send( null );
   return xmlHttp.responseText;
}

function initialize() {
   // default position: Fribourg
   //var myLatlng = new google.maps.LatLng(46.800590,7.150040);
   // default position: Atlantic :)
   var myLatlng = new google.maps.LatLng(27.839076,-33.75);
   var mapOptions = {
       zoom: 3,
       center: myLatlng,
       mapTypeId: google.maps.MapTypeId.ROADMAP
   };
   map = new google.maps.Map(document.getElementById('map-canvas'), mapOptions);

   // get all the ips of all bridges
   response = httpGet("http://"+SERVER_IP+":"+ERS_SERVER_PORT+"/"+ERS_PATH+"/query_bridges");
   ips = response.split(/\r?\n/);

   var infoWindowAr = new Array(); 
   var markerAr = new Array(); 
   for( var i=0; i<ips.length-2; ++i) { 
       ip_timestamp = ips[i].split(' ');
       ip = ip_timestamp[0]; 
       timestamp = ip_timestamp[1];
       // now ge tthe lat,lng for this ip 
       response = httpGet("http://"+SERVER_IP+":"+ERS_GEO_PORT+"/geolocation.php?ip="+ip);
       if ( response.length < 4 ) 
          continue;
       latlngct = response.split(' '); 

       var contentString = '<div id="content">'+
             'IP: ' + ip +
             '</div>';
       infoWindowAr[i] = new google.maps.InfoWindow({
            content: contentString
       });
       new_bridge = new google.maps.LatLng(latlngct[0], latlngct[1]);

       markerAr[i] = new google.maps.Marker({
            position: new_bridge,
            map: map,
            title: 'Bridge '+i,
            idx: i,
            max_idx: ips.length-2,
            ip: ip,
            first_sync: convertTimestampToDate(timestamp),
            id: "marker"
       });
       google.maps.event.addListener(markerAr[i], 'click', function() {
          for( var k=0; k<this.max_idx; ++k) {
               if( infoWindowAr[k] != null ) 
                  infoWindowAr[k].close();
         }
          infoWindowAr[this.idx].open(map, markerAr[this.idx]);
          google.maps.event.addListener(infoWindowAr[this.idx],'closeclick',function(){
            document.getElementById("plot").innerHTML="";
            document.getElementById("bridge_details").innerHTML="<i>Please choose one of the bridges from the map.</i>";
            document.getElementById("bridge_stats").innerHTML="";
          });
          getBridgeGeolocationDetails(this.ip, this.first_sync, this.title);
          getTotals(this.ip);
          // default, show entities synched for the last 5 mins
          document.getElementById("history").value = 5;
          document.getElementsByName("plot_type")[1].checked=true;
          chart(this.ip, 5, 'NUM');
       });
   }
}
//google.maps.event.addDomListener(window, 'load', initialize);

function getBridgeGeolocationDetails(ip, first_sync, title) { 
   response = httpGet("http://"+SERVER_IP+":"+ERS_GEO_PORT+"/geolocation.php?ip="+ip+"&all");
   geo_d = response.split(','); 
   var showDetails = '<div>'+
         'Bridge name: '+title+
         '<br/>First synch date (UTC): '+first_sync;
   if( geo_d[1].length > 0 )
         showDetails += '<br/>Country name: '+geo_d[1];
   if( geo_d[2].length > 0 )
         showDetails += '<br/>Region: ' + geo_d[2];
   if( geo_d[3].length > 0 )
         showDetails += '<br/>Region name: '+ geo_d[3];
   if( geo_d[4].length > 0 )
         showDetails += '<br/>City: '+ geo_d[4];
   if( geo_d[5].length > 0 )
         showDetails += '<br/>Continent code: '+geo_d[5];
      showDetails += '</div>';
   document.getElementById('bridge_details').innerHTML = showDetails; 
   document.getElementById('hidden_ip').value=ip;   
}

function getTotals(ip) { 
   response = httpGet("http://"+SERVER_IP+":"+ERS_SERVER_PORT+"/"+ERS_PATH+"/query_bridges?ip="+ip);
   bridge_total_stats = response.split(/\r?\n/)[0].split(' ');

   total_keyspaces = bridge_total_stats[2];
   total_entities = bridge_total_stats[3];
   total_bytes = bridge_total_stats[4];
   var stats = '<div>' + 
      '<b>Total</b> graphs synched: '+total_keyspaces+
      '<br/><b>Total</b> entities synched: '+total_entities+
      '<br/><b>Total</b> bytes synched: '+total_bytes+
      '</div>';
   document.getElementById('bridge_stats').innerHTML = stats;
}

function updatePlot() {
   var ip = document.getElementById("hidden_ip").value;
   if( ip.length == 0 ) {
      alert("Please firstly choose a bridge from the map.");
      return;
   }
   var timespan = document.getElementById("history").value;
   var rads = document.getElementsByName("plot_type");
   var plot_type;
   for (var i=0; i < rads.length; i++)
      if (rads[i].checked)
          plot_type = rads[i].value;
   chart(ip, timespan, plot_type); 
}
