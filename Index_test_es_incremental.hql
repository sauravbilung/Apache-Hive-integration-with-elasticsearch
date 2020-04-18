--------------------------------- Pre Processing --------------------------------------

ADD JAR hdfs://hadoopnn/location_of_jar_file/elasticsearch-hadoop-hive-7.5.0.jar;
ADD JAR hdfs://hadoopnn/location_of_jar_file/commons-httpclient-3.0.1.jar;


--------------------- Final table pointing to Elastic Search Index --------------------

CREATE EXTERNAL TABLE IF NOT EXISTS es_final(
id INT,
name String
)
ROW FORMAT SERDE 'org.elasticsearch.hadoop.hive.EsSerDe'
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(
'es.resource' = 'es_test/es_test',
'es.nodes'= 'IP address' ,
'es.port'='9200',
'es.nodes.wan.only' = 'true', 
'es.batch.write.retry.count'='-1',
'es.batch.write.retry.wait'='2',
'es.bulk.size.bytes'='50',
'es.bulk.size.entries'='200',
'es.index.auto.create' = 'true',
'es.index.read.missing.as.empty' = 'true',
'es.write.rest.error.handler.log.logger.level'='ERROR',
'es.write.rest.error.handlers'='log',
'es.write.rest.error.handler.log.logger.name'='BulkErrors');

----------- Prefinal table which has similar data as above. Used for identifying latest records -----------

CREATE TABLE IF NOT EXISTS es_prefinal(
id INT,
name String
);

INSERT into es_prefinal
SELECT
id,
name
FROM db_name.input_table;

------------------ Identifying latest records and adding them to final table for indexing -----------------------

INSERT INTO es_final 
SELECT id,name from es_prefinal group by id,name having count(*)=1;

---------- Removing duplicates in the prefinal table ------------------------------------------------------------

truncate table es_prefinal;

INSERT INTO es_prefinal 
SELECT id,name from db_name.input_table;

