package com.cloudera.flink.ts.gen.connector;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

public class HeartRateGeneratorSourceTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(
                "CREATE TABLE server_logs ( \n"
                        + "    pharma_row_id BIGINT,\n"
                        + "    msg_type STRING, \n"
                        + "    user_id STRING, \n"
                        + "    patch_id STRING, \n"
                        + "    ts_timestamp BIGINT, \n"
                        + "    beats_per_minute DOUBLE \n"
                        + ") WITH (\n"
                        + "  'connector' = 'ts_gen', \n"
                        + "  'avro_schema_file_name' = 'heartrate.avro',\n"
                        + "  'avro_schema_location' = '/Users/njayakumar/Desktop/ak/naga/workspace/demos/ts-gen-connector/src/main/resources/',\n"
                        + "  'ts_schema_file_name' = 'heartrate.json',\n"
                        + "  'ts_schema_location' = '/Users/njayakumar/Desktop/ak/naga/workspace/demos/ts-gen-connector/src/main/resources/' \n"
                       // + "  'generation' = '100'"
                        + ")");

        TableResult tableResult = tEnv.executeSql("SELECT * FROM server_logs");

        CloseableIterator<Row> collect = tableResult.collect();

        Row row1 = collect.next();
        System.out.println(row1);
        Row row2 = collect.next();
        System.out.println(row2);
        Row row3 = collect.next();
        System.out.println(row3);

        for (int i = 1; i <100 ; i++ ){
            System.out.println( collect.next());
        }


    }


}
