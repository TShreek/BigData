student_data = LOAD 'student.txt' USING
PigStorage(',') AS (id:int, name:chararray, lastname:chararray, age:int, phone:chararray, city:chararray);
filter_data = FILTER student_data BY city == 'chennai';
grouped_data = GROUP student_data BY age;
STORE filter_data INTO 'Filter'
STORE groouped_data INTO 'Grouped'
