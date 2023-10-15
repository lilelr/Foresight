hdfs dfs -du -s -h /BenchmarkData/Tpcds/tpcds_500/tpcds_parquet/web_sales
/BenchmarkData/Tpcds/tpcds_1000/tpcds_parquet
hdfs dfs -du -s -h /BenchmarkData/Tpcds/tpcds_1000/tpcds_parquet


lemaker@slave4:~$ hdfs dfs -du -s -h /BenchmarkData/Tpcds/tpcds_500/tpcds_parquet/web_sales
26.5 G  26.5 G  /BenchmarkData/Tpcds/tpcds_500/tpcds_parquet/web_sales
lemaker@slave4:~$ hdfs dfs -du -s -h /BenchmarkData/Tpcds/tpcds_100/tpcds_parquet/web_sales
5.3 G  5.3 G  /BenchmarkData/Tpcds/tpcds_100/tpcds_parquet/web_sales
26.5/5.3 =5

desc web_sales;  34 columns

hdfs dfs -du -s -h /BenchmarkData/Tpcds/tpcds_500/tpcds_parquet/store_returns

lemaker@slave4:~$ hdfs dfs -du -s -h /BenchmarkData/Tpcds/tpcds_500/tpcds_parquet/store_returns
8.3 G  8.3 G  /BenchmarkData/Tpcds/tpcds_500/tpcds_parquet/store_returns
lemaker@slave4:~$ hdfs dfs -du -s -h /BenchmarkData/Tpcds/tpcds_100/tpcds_parquet/store_returns
1.7 G  1.7 G  /BenchmarkData/Tpcds/tpcds_100/tpcds_parquet/store_returns
8.3/1.7 = 4.8

desc store_returns;  20 columns

hdfs dfs -du -s -h /BenchmarkData/Tpcds/tpcds_500/tpcds_parquet/catalog_sales
56.7 G  56.7 G  /BenchmarkData/Tpcds/tpcds_500/tpcds_parquet/catalog_sales
catalog_sales 34 columns


lemaker@slave4:~$ hdfs dfs -du -s -h /BenchmarkData/Tpcds/tpcds_500/tpcds_parquet/store_sales
70.0 G  70.0 G  /BenchmarkData/Tpcds/tpcds_500/tpcds_parquet/store_sales

view the block information of a hdfs file:

hdfs fsck /BenchmarkData/Tpcds/tpcds_1000/tpcds_parquet/store_sales -files -blocks -locations

1200
hdfs fsck /BenchmarkData/Tpcds/tpcds_1000/tpcds_parquet/store_returns -files -blocks -locations

240



23 columns

## hive sql
use tpcds_500_parquet
desc formatted catalog_sales;
desc formatted store_sales;
desc store_sales;

desc catalog_sales;

 

show databases;
show tables;

ANALYZE TABLE catalog_sales COMPUTE STATISTICS;
ANALYZE TABLE catalog_sales COMPUTE STATISTICS NOSCAN;

https://docs.databricks.com/sql/language-manual/sql-ref-syntax-aux-analyze-table.html

DESC EXTENDED catalog_sales;

https://aws.amazon.com/cn/ec2/instance-types/

### TPC-H
show databases;
use tpch_500_parquet;

Table list:

customer
lineitem
nation
orders
part
partsupp
region
supplier

suppose that we want to know the number of columns and size of Table supplier;
1. use hive sql "desc formatted supplier" to get the hdfs path of the table
2. use the following hdfs command
hdfs dfs -du -s -h hdfs://slave4:9000/BenchmarkData/Tpch/tpch_500/tpch_parquet/supplier
384.7 M  384.7 M

ANALYZE TABLE supplier COMPUTE STATISTICS;

desc formatted customer;
hdfs dfs -du -s -h hdfs://slave4:9000/BenchmarkData/Tpch/tpch_500/tpch_parquet/customer
8 5.9MB

desc formatted lineitem;
hdfs dfs -du -s -h hdfs://slave4:9000/BenchmarkData/Tpch/tpch_500/tpch_parquet/lineitem
16 117.4GB

desc formatted nation;
hdfs dfs -du -s -h hdfs://slave4:9000/BenchmarkData/Tpch/tpch_500/tpch_parquet/nation
4 3MB

desc formatted orders;
hdfs dfs -du -s -h hdfs://slave4:9000/BenchmarkData/Tpch/tpch_500/tpch_parquet/orders
9 32GB

desc formatted part;
hdfs dfs -du -s -h hdfs://slave4:9000/BenchmarkData/Tpch/tpch_500/tpch_parquet/part
9 3.2GB

partsupp
desc formatted partsupp;
hdfs dfs -du -s -h hdfs://slave4:9000/BenchmarkData/Tpch/tpch_500/tpch_parquet/partsupp

5 21.4GB

region

desc formatted region;
hdfs dfs -du -s -h hdfs://slave4:9000/BenchmarkData/Tpch/tpch_500/tpch_parquet/region

3 1.5MB

## TPC-H

Tables order

OK
# col_name            	data_type           	comment             
	 	 
o_orderkey          	bigint              	                    
o_custkey           	bigint              	                    
o_orderstatus       	string              	                    
o_totalprice        	double              	                    
o_orderdate         	string              	                    
o_orderpriority     	string              	                    
o_clerk             	string              	                    
o_shippriority      	int                 	                    
o_comment           	string  

## cardinality 

select length(o_comment) from orders;

https://stackoverflow.com/questions/19128940/what-is-the-difference-between-partitioning-and-bucketing-a-table-in-hive

### backup spark tunner log
hadoop fs -get hdfs://slave4:9000/sparktunerLogs sparktunerLogs-06-20

### TPC-DS Q1 500G
http://slave4:18080/history/application_1682218828348_0029/jobs/job/?id=2
100 GB
http://slave4:18080/history/application_1679304940725_0014/jobs/job/?id=2

synthetic q_dag
http://slave4:18080/history/application_1687158624101_0013/jobs/job/?id=2

### TPC-DS Q3 100G
http://slave4:18080/history/application_1687158624101_0016/stages/

synthetic q_dag
http://slave4:18080/history/application_1687158624101_0015/jobs/
