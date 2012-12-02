#!/bin/bash

<<COM
if [ $# -lt 2 ]; then 
   echo "check the usage of this script !" 
   exit 1
fi
COM


FILE1=q_by_subject_time_10_100.log
OUTPUT_FILE=plot_q_time_10_100

echo "Output file $OUTPUT_FILE"
gnuplot -persist << EOF 
set term post eps
set style data histogram
set style fill solid
set term png
set output "${OUTPUT_FILE}.png"
set xlabel "# of queries"
set auto x 
set ylabel "Total time(sec)" 
set yrange [0:*]
set key left top
set xtic rotate by 90 scale 0
set boxwidth 0.8
plot "${FILE1}" using 2:xtic(1) title col linecolor rgb "#0000ff",\
     "${FILE1}" using 3:xtic(1) title col linecolor rgb "#000099", \
     "${FILE1}" using 4:xtic(1) title col linecolor rgb "#00ff00", \
     "${FILE1}" using 5:xtic(1) title col linecolor rgb "#009900", \
     "${FILE1}" using 6:xtic(1) title col linecolor rgb "#ff0000", \
     "${FILE1}" using 7:xtic(1) title col linecolor rgb "#990000"
EOF
#set title "Querying by SUBJECT response time on LD-in-Couch"
