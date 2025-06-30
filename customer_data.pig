customers = LOAD 'customers.txt' USING
PigStorage(',') AS (id:int, name:chararray, age:int, address:chararray, salary:int);

orders = LOAD 'order.txt' USING 
PigStorage(',') as (oid:int, date:chararray, customer_id:int, amount:int);

join_res = JOIN customers by id, orders by customer_id;
STORE join_res INTO result_join;
sorting = ORDER customers by age ASC;
STORE sorting INTO sorted_result;
