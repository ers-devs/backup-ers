#!/bin/bash 
#
# Run random get queries on CouchDB.
#
#

if [ $# -lt 4 ]; then 
	echo "<input file> <couchdb_name> <couchdb_graph_name> <no_queries>" 
	exit -1
fi

INPUT_FILE=$1
COUCHDB_NAME=$2
COUCHDB_GRAPH_NAME=$3
NO_QUERIES=$4

OUTPUT_FILE_NAME="${2}-${3}-${4}.time"
rm $OUTPUT_FILE_NAME 2> /dev/null

q_ran=0
bad_q=0
no_total_res=0
while true
do
	orig_subject=$(shuf -n 1 ${INPUT_FILE} | cut -d " "  -f 1)
	subject=$(echo $orig_subject | sed -e 's/:/%3A/g' | sed -e 's/#/%23/g' | sed -e 's/\//%2F/g')
	#echo $subject
        len=${#subject} 
	len=$((len - 2))
	rm tmp 2> /dev/null
	# now query the DB 
	{ time curl -X GET 'http://localhost:5984/'$COUCHDB_NAME'/'${subject:1:$len}''; } &> tmp
#	cat tmp

	#check it was a successful run 
	any_row=$(cat tmp | grep "error" | wc -l)
	if [ $any_row -eq 0 ]; then 
		#it was a good query 
		q_ran=$((q_ran+1))
		# so get the running time 
		cat tmp | grep "real" >> $OUTPUT_FILE_NAME 
		no_res=$(cat tmp | grep "{\"_id\"" | wc -l)
		no_total_res=$((no_total_res+no_res))
	else 
		bad_q=$((bad_q+1))
#BE AWARE: if you use 1mil.nt file , there are only 700k triples loaded so you might end up here often !!! 
#		echo "$orig_subject   -> BAD , perhaps JSON scape not right   : $subject " 
#		echo 'http://localhost:5984/'$COUCHDB_NAME'/_design/entity/_view/by_subject?key="'${subject:1:$len}${COUCHDB_GRAPH_NAME}'"'
	fi
	if [ $q_ran -eq $NO_QUERIES ]; then 
		# all queries have run 
		break
	fi
done 

rm tmp
echo "ALL $NO_QUERIES have run successfully. Please check $OUTPUT_FILE_NAME for output data !"
echo "Bad queries = $bad_q "

# now get the actual running time by adding all querying time 
cat $OUTPUT_FILE_NAME | sort | cut -d "m" -f 2 | cut -d "s" -f 1 > tmp 
total_time=$(awk '{sum+=$1}END{print sum}' tmp)
echo "Total querying time is:  ${total_time} " 
echo -n "   per query time: "
echo "$total_time / $NO_QUERIES" | bc -l
echo "Total results: ${no_total_res} "
echo -n "   per result time: " 
echo "$total_time / $no_total_res" | bc -l 
rm tmp

#cat $OUTPUT_FILE_NAME | more  
rm $OUTPUT_FILE_NAME
