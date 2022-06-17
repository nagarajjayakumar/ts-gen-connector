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

