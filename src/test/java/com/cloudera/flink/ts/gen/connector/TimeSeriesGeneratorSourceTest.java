package com.cloudera.flink.ts.gen.connector;


import java.util.List;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;

public class TimeSeriesGeneratorSourceTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(
                "CREATE TABLE server_logs ( \n"
                        + "    transaction_id BIGINT,\n"
                        + "    card_id BIGINT, \n"
                        + "    user_id STRING, \n"
                        + "    purchase_id BIGINT, \n"
                        + "    store_id BIGINT, \n"
                        + "    torque BIGINT \n"
                        + ") WITH (\n"
                        + "  'connector' = 'ts_gen', \n"
                        + "  'avro_schema_file_name' = 'transactions.avro',\n"
                        + "  'avro_schema_location' = '/Users/njayakumar/Desktop/ak/naga/workspace/demos/ts-gen-connector/src/main/resources/',\n"
                        + "  'ts_schema_file_name' = 'basicConfig.json',\n"
                        + "  'ts_schema_location' = '/Users/njayakumar/Desktop/ak/naga/workspace/demos/ts-gen-connector/src/main/resources/' \n"
                        + ")");

        TableResult tableResult = tEnv.executeSql("SELECT transaction_id, torque FROM server_logs");

        CloseableIterator<Row> collect = tableResult.collect();

        Row row1 = collect.next();
        System.out.println(row1);
        Row row2 = collect.next();
        System.out.println(row2);
        Row row3 = collect.next();
        System.out.println(row3);
    }


}
