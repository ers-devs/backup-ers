#!/bin/bash 
#
# Run random get queries by ID given a range (start, end) on CouchDB.
#
#

if [ $# -lt 3 ]; then 
	echo "<input file> <couchdb_name> <no_queries>" 
	exit -1
fi

INPUT_FILE=$1
COUCHDB_NAME=$2
NO_QUERIES=$3
LIMIT_Q=1000

OUTPUT_FILE_NAME="${2}-${3}-by_id_range.time"
rm $OUTPUT_FILE_NAME 2> /dev/null

q_ran=0
bad_q=0
while true
do
	id_start=$(shuf -n 1 ${INPUT_FILE} )
	id_end=$(shuf -n 1 ${INPUT_FILE} )
	rm tmp 2> /dev/null
	# now query the DB 
# (start,end,limit)
#	{ time curl -X GET 'http://localhost:5984/'$COUCHDB_NAME'/_all_docs?start_key="'${id_start}'"&end_key="'${id_end}'"&include_docs=true&limit=2000'; } &> tmp
# (start,limit)
	{ time curl -X GET 'http://localhost:5984/'$COUCHDB_NAME'/_all_docs?start_key="'${id_start}'"&include_docs=true&limit='$LIMIT_Q; } &> tmp
	#check it was a successful run 
	any_row=$(cat tmp | grep "query_parse_error" | wc -l)
	if [ $any_row -eq 0 ]; then 
		# get no of rows returned 
		no_rows_result=$(cat tmp | wc -l)
		no_rows_result=$((no_rows_result-9))
		if [ $no_rows_result -lt $LIMIT_Q ]; then
			bad_q=$((bad_q+1))
			continue
		fi
		#it was a good query 
		q_ran=$((q_ran+1))
		# so get the running time 
		running_time=$(cat tmp | grep "real")
		echo "$running_time $no_rows_result" >> $OUTPUT_FILE_NAME 
	else 
		bad_q=$((bad_q+1))
#BE AWARE: if you use 1mil.nt file , there are only 700k triples loaded so you might end up here often !!! 
#		echo "$orig_subject   -> BAD , perhaps JSON scape not right   : $subject " 
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
cat $OUTPUT_FILE_NAME | sort | cut -d " " -f 2  > tmp 
total_no_res=$(awk '{sum+=$1}END{print sum}' tmp)
echo "Total querying time is:  ${total_time} " 
echo -n "   per query time: "
echo "$total_time / $NO_QUERIES" | bc -l
echo "Total docs retrieved: ${total_no_res} "
echo -n "   per document time: " 
echo "$total_time / $total_no_res" | bc -l 

rm tmp

#cat $OUTPUT_FILE_NAME 
rm $OUTPUT_FILE_NAME
