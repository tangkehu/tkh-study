CREATE TABLE sourceTable (
    `data` STRING
) WITH (
    'connector.type' = 'elasticsearch',
    'connector.version' = '5',
    'connector.hosts' = 'http://localhost:9200',
    'connector.index' = 'flink-demo',
    'connector.document-type' = 'ip',
    'format.type'='json'
)

CREATE TABLE sinkTable (
    `data` STRING
) WITH (
    'connector.type' = 'elasticsearch',
    'connector.version' = '6',
    'connector.hosts' = 'http://localhost:9200',
    'connector.index' = 'flink-one',
    'connector.document-type' = 'ip',
    'update-mode' = 'append',
--     'connector.bulk-flush.max-actions' = '1',
    'format.type' = 'json'
)

INSERT INTO sinkTable SELECT * FROM sourceTable