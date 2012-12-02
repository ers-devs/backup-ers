if #arg < 2 then
   print("parse_log_ld-in-couch <input_file> <output_file> <total_no_tuples>") 
   os.exit() 
end

-- open input file 
io.input(arg[1])
-- open output file 
io.output(arg[2])

-- read line by line 
first_read = false
inserted_tuples = 0
-- get total number of tuples that have been used 
total_tuples = arg[3]

for line in io.lines() do
   time, tmp, node_id = string.match(line, "(%d+:%d+:%d+)%s*(DEBUG #)(%d*)") 

   inserted_tuples = inserted_tuples + 1
   hour, min, sec = string.match(time, "(%d+):(%d+):(%d+)")
--   print(hour,min,sec,ms)
   if not first_read then 
      -- first timestamp in the file (everyting is relative to this)
      f_timestamp = (3600*hour)+(60*min)+sec
      first_read = true
   end
   -- get the timestamp read from the current line 
   cur_timestamp = (3600*hour)+(60*min)+sec
   diff = cur_timestamp - f_timestamp

   io.write(diff," ", inserted_tuples, " ", (inserted_tuples/total_tuples),"\n")
end

