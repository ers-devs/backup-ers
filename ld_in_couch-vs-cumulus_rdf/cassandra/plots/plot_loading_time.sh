#!/bin/bash

<<COM
if [ $# -lt 2 ]; then 
   echo "check the usage of this script !" 
   exit 1
fi
COM


#FILE1=$1
FILE1=loading_time.log 

OUTPUT_FILE=loading_time

echo "Output file $OUTPUT_FILE"
gnuplot -persist << EOF 
set output "${OUTPUT_FILE}.png"
set term post eps
set style data histogram
set style fill solid
set term png
set auto x 
set ylabel "Loading time (sec)" 
set xtic rotate by 90 scale 0
set yrange [0:*]
plot  "${FILE1}" using 2:xtic(1) title col  linecolor rgb "#0000ff"
EOF


#set title "Loading time on LD-in-Couch"
