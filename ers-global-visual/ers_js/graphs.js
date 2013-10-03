function chart(ip, span, stat) { 
   
        var y_axis_text;
        var tooltip_text; 
        if( stat == 'NUM' ) {
            y_axis_text = 'number of entities'; 
            tooltip_text = 'entities';
         }
        else { 
            y_axis_text = 'number of bytes';
            tooltip_text = 'bytes'; 
        }
        var options = {
            chart: {
                  type: 'spline',
                  renderTo: 'plot'
            },
            title: {
              text: 'Synchronized bytes by brige with IP: '+ip
            },
            xAxis: {
                type: 'datetime',
            },
            yAxis: {
              title: {
                  text: y_axis_text
              },
               min: 0
             },
            tooltip: {
                formatter: function() {
                        return '<b>'+ this.series.name +'</b><br/>'+
                        Highcharts.dateFormat('%e. %b ', this.x) +': '+ this.y + tooltip_text;
                }
             },
             series: []
        }; 

   $.get('http://'+SERVER_IP+':'+ERS_SERVER_PORT+'/'+ERS_PATH+'/query_bridges_stats?ip='+ip+'&span='+span+'&stat='+stat, function(data) {
       // Split the lines
       var lines = data.split('\n');
       if( lines[0].length == 0 ) {
         var text = "<i>No data to plot for the last " + span + " minutes for bridge " + ip;
         text += "<br/> Try a different time span or bridge.</i> ";
         document.getElementById("plot").innerHTML = text;
         return;
       }

       // Iterate over the lines and add categories or series
       $.each(lines, function(lineNo, line) {
            if( line.length == 0 ) 
               return;
            var tmp = line.split(':');
            var keyspace = tmp[0]; 
            var items = tmp[1].split(' '); 
            var series = {
                data: [],
                name: 'Graph '+keyspace.substr(1,keyspace.length-2)
            };
            var timestamps = new Array();
            var payload = new Array();
            var j=0;
            $.each(items, function(itemNo, item) {
                if( item.length == 0 ) 
                  return;
                var data = item.split(',');
                timestamps[j] = data[0].substr(2, data[0].length-3);
                payload[j] = data[1].substr(1, data[1].length-3);
                var now = new Date(timestamps[j]*1);
                var now_utc = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(),  now.getUTCHours(), now.getUTCMinutes(), now.getUTCSeconds());
                series.data.push([now_utc, parseFloat(payload[j])]);
                j = j+1;

            });
            options.series.push(series);
            
       });
      // Create the chart
      var chart = new Highcharts.Chart(options);
   });
}
