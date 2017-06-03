/*
hdfs dfs -mkdir /user/cloudera/cust
hdfs dfs -mkdir /user/cloudera/sales
hdfs dfs -put customers.csv /user/cloudera/cust/
hdfs dfs -put sales.csv /user/cloudera/sales/
*/

create database sample1;

create external table sample1.cust (
 customer_id int,
 name string,
 street string,
 city string,
 state string,
 zip int
)
row format delimited fields terminated by ','
location '/user/cloudera/cust';

create external table sample1.sales (
 ts string,
 customer_id int,
 price decimal
)
row format delimited fields terminated by ','
location '/user/cloudera/sales';

create external table sample1.results (
 state string,
 sales decimal
)
row format delimited fields terminated by ','
location '/user/cloudera/results_hive';

insert overwrite table sample1.results
select state,sum(price) from sample1.cust c inner join sample1.sales s on s.customer_id = c.customer_id
group by state;
