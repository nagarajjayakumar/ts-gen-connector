# ts-gen-connector

# 
SQL – Config
=================

#
```
Time Series table source
===========================

CREATE TABLE `heartrate_test_events_raw` (
 pharma_row_id BIGINT,
 msg_type STRING,
 user_id STRING,
 patch_id STRING,
 ts_timestamp BIGINT,
 beats_per_minute DOUBLE
 
) WITH (
'connector' = 'ts_gen',
'avro_schema_file_name' = 'heartrate.avro',
'ts_schema_location' = '/tmp',
'avro_schema_location' = '/tmp',
'ts_schema_file_name' = 'heartrate.json'
)

```

```
Kafka Table Source
====================

CREATE TABLE heartrate_test_events (
 msg_yr bigint,
 pharma_row_id BIGINT,
 msg_type STRING,
 user_id STRING,
 patch_id STRING,
 ts_timestamp BIGINT,
 acquisition_timestamp timestamp,
 beats_per_minute DOUBLE,
 msg_dt string,
 msg_mth string,
 ingest_ts timestamp, 
 PRIMARY KEY (pharma_row_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'property-version' = 'universal',
  'properties.bootstrap.servers' = '52.206.57.10:9092',
  'topic' = 'heartrate-test-events',
  'value.format' = 'json',
  'key.format' = 'json',
  'properties.group.id' = 'heartrate-test-working-group'
);


```

#
Deployment - Notes
===================

```
ssh -i workshop.pem centos@44.206.13.121


kafka-topics --create --topic heartrate-test-events --bootstrap-server 52.206.57.10:9092

kafka-topics --bootstrap-server=52.206.57.10:9092 --list

kafka-topics --bootstrap-server=52.206.57.10:9092 --describe --topic heartrate-test-events

kafka-console-consumer  --bootstrap-server=52.206.57.10:9092 --topic heartrate-test-events --from-beginning



CREATE TABLE heartrate_test_events (
 msg_yr bigint,
 pharma_row_id BIGINT,
 msg_type STRING,
 user_id STRING,
 patch_id STRING,
 ts_timestamp BIGINT,
 acquisition_timestamp timestamp,
 beats_per_minute DOUBLE,
 msg_dt string,
 msg_mth string,
 ingest_ts timestamp, 
 PRIMARY KEY (pharma_row_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'property-version' = 'universal',
  'properties.bootstrap.servers' = '52.206.57.10:9092',
  'topic' = 'heartrate-test-events',
  'value.format' = 'json',
  'key.format' = 'json',
  'properties.group.id' = 'heartrate-test-working-group'
);

```


```
FLINK SQL - For streaming data to KAFKA SINK

SET 'table.local-time-zone' = 'UTC';



INSERT INTO heartrate_test_events
SELECT extract(YEAR from TO_TIMESTAMP_LTZ(ts_timestamp, 3)) as msg_yr, 
pharma_row_id, msg_type, 
user_id,patch_id,ts_timestamp, TO_TIMESTAMP_LTZ(ts_timestamp,3) as acquisition_timestamp, beats_per_minute,
DATE_FORMAT(TO_TIMESTAMP_LTZ(ts_timestamp, 3),'yyyy-MM-dd') as msg_dt, DATE_FORMAT(TO_TIMESTAMP_LTZ(ts_timestamp, 3),'yyyy-MM') as msg_mth,
                                           TO_TIMESTAMP(DATE_FORMAT(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss')) as ingest_ts from heartrate_test_events_raw;


```