package com.cloudera.flink.ts.gen.connector;

import be.cetic.tsimulus.config.Configuration;
import com.cloudera.flink.ts.gen.connector.common.TimeSeriesGeneratorSourceOptions;
import io.confluent.avro.random.generator.Generator;
import org.apache.commons.io.FileUtils;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Random;

public class TimeSeriesGeneratorSource extends RichSourceFunction<RowData> {
    private final TimeSeriesGeneratorSourceOptions options;
    private final Map<String, DataType> physicalColumnNameToDataTypeMap;

    private transient volatile Configuration tsSchemaConfiguration;
    private transient volatile Generator avroGenerator;

    private long seed = 100L;
    private long generation = 10000L;

    private transient volatile boolean running = false;

    public TimeSeriesGeneratorSource (
            TimeSeriesGeneratorSourceOptions options,
            Map<String, DataType> physicalColumnNameToDataTypeMap
    ) {
        this.options = options;
        this.physicalColumnNameToDataTypeMap = physicalColumnNameToDataTypeMap;
    }

    @Override
    public void run(SourceFunction.SourceContext<RowData> ctx) throws Exception {
        init();
        running = true;
        while (running) {
            // Trigger some TSIMULUS api to generate the records
            TsGenUtil.generateRecord(tsSchemaConfiguration, avroGenerator, physicalColumnNameToDataTypeMap, ctx);

        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    private void init() throws Exception {

        if(options.getSeed() != null) {
            this.seed = options.getSeed();
        }

        if(options.getGeneration() != null) {
            this.generation = options.getGeneration();
        }

        String tsSchemaLocation = options.getTsSchemaLocation();
        String tsSchemaFileName = options.getTsSchemaFileName();
        String tsSchema = getSchema(tsSchemaLocation, tsSchemaFileName);
        tsSchemaConfiguration = TsGenUtil.getConfig(tsSchema);

        // avro schema

        String avroSchemaLocation = options.getAvroSchemaLocation();
        String avroSchemaFileName = options.getAvroSchemaFileName();
        String avroSchema = getSchema(avroSchemaLocation, avroSchemaFileName);

        avroGenerator = new Generator.Builder()
                .schemaString(avroSchema).random(new Random(seed))
                .generation(generation)
                .build();

    }

    private String getSchema(String schemaLocation, String schemaFileName) throws IOException {
        String schemaPath = schemaLocation + "/" + schemaFileName;
        File file = new File(schemaPath);
        String schema = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
        return schema;
    }


}
