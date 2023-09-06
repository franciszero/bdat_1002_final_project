--CREATE EXTERNAL TABLE `articles`(
--  `author` string,
--  `title` string,
--  `description` string,
--  `url` string,
--  `urltoimage` string,
--  `publishedat` string,
--  `content` string,
--  `source_name` string)
--ROW FORMAT SERDE
--  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
--WITH SERDEPROPERTIES (
--  'field.delim'='',
--  'serialization.format'='')
--STORED AS INPUTFORMAT
--  'org.apache.hadoop.mapred.TextInputFormat'
--OUTPUTFORMAT
--  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
--LOCATION
--  'hdfs://localhost:9000/final10002/db1/table1'
--TBLPROPERTIES (
--  'bucketing_version'='2',
--  'transient_lastDdlTime'='1692258219')
show create table articles;

select author, count(*) from articles group by author;


