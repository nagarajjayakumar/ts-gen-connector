# ts-gen-connector

# 
SQL – Config
=================

#
```
CREATE TABLE `ts_gen_table` (
`transaction_id` BIGINT,
`card_id` BIGINT,
`user_id` VARCHAR(2147483647),
`purchase_id` BIGINT,
`store_id` BIGINT,
`torque` DOUBLE
) WITH (
'connector' = 'ts_gen',
'avro_schema_file_name' = 'transactions.avro',
'ts_schema_location' = '/tmp',
'avro_schema_location' = '/tmp',
'ts_schema_file_name' = 'basicConfig.json'
)
```

#
Deployment - Notes
===================

```
kafka-topics --create --topic patchpair-events --bootstrap-server cdp.44.206.13.121.nip.io:9092

kafka-topics --describe --topic patchpair-events --bootstrap-server cdp.44.206.13.121.nip.io:9092

kafka-topics --bootstrap-server=cdp.44.206.13.121.nip.io:9092 --list

kafka-topics --bootstrap-server=cdp.44.206.13.121.nip.io:9092 --describe --topic patchpair-events

ssh -i workshop.pem centos@44.206.13.121


kafka-console-consumer --bootstrap-server cdp.54.161.94.194.nip.io:9092 --topic iot_enriched --from-beginning


CREATE TABLE patchpair_events (
`transaction_id` BIGINT,
`card_id` BIGINT,
`user_id` VARCHAR,
`purchase_id` BIGINT,
`store_id` BIGINT,
`torque` DOUBLE,
PRIMARY KEY (transaction_id) NOT ENFORCED
) WITH (
'connector' = 'upsert-kafka',
'property-version' = 'universal',
'properties.bootstrap.servers' = 'cdp.54.161.94.194.nip.io:9092',
'topic' = 'iot_enriched',
'value.format' = 'json',
'key.format' = 'json',
'properties.group.id' = 'my-working-group'
);

```
