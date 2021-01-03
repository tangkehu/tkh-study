CREATE TEMPORARY TABLE sourceTbale
USING org.elasticsearch.spark.sql
OPTIONS (resource_read 'your-index/your-type', nodes '127.0.0.1:9200')

SELECT * FROM sourceTbale

SINK 127.0.0.1:9200 out-index/out-type
