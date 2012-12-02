#!/bin/bash

<<COM
if [ $# -lt 2 ]; then 
   echo "check the usage of this script !" 
   exit 1
fi
COM


#FILE1=$1
FILE1=10k_time.plot.data 
FILE2=100k_time.plot.data  
FILE3=700k_time.plot.data

OUTPUT_FILE=diff_loading_time

echo "Output file $OUTPUT_FILE"
gnuplot -persist << EOF 
set term post eps
set term png
set output "${OUTPUT_FILE}.png"
set xlabel "Time(sec)"
set format x "%.0f"
set ylabel "Proportion of loaded triples" 
set yrange [0:1]
plot  "${FILE1}" using 1:3 smooth bezier with lines title "10k triples", \
      "${FILE2}" using 1:3 smooth bezier with lines title "100k triples", \
      "${FILE3}" using 1:3 smooth bezier with lines title "0.7m triples"
EOF


#set title "Loading time on LD-in-Couch"
