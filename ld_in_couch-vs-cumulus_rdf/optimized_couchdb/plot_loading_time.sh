#!/bin/bash

<<COM
if [ $# -lt 2 ]; then 
   echo "check the usage of this script !" 
   exit 1
fi
COM

FILE1=loading_time+size_on_disk.log 
OUTPUT_FILE=loading_time_comp

echo "Output file $OUTPUT_FILE"
gnuplot -persist << EOF 
set output "${OUTPUT_FILE}.png"
set term post eps
set style data histogram
set style fill solid
set key top righ
set term png
set auto x 
set ylabel "Loading time (sec) - LOG SCALE" 
set logscale y
#set xtic rotate by 90 scale 0
set yrange [1:*]
set boxwidth 0.8
plot  "${FILE1}" using 8:xtic(1) title col  linecolor rgb "#0000ff",\
      "${FILE1}" using 4:xtic(1) title col  linecolor rgb "#ff4030",\
      "${FILE1}" using 6:xtic(1) title col  linecolor rgb "#00ff00"
EOF


#set title "Loading time on LD-in-Couch"
