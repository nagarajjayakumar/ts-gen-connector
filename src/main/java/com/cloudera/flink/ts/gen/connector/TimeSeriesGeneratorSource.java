package com.cloudera.flink.ts.gen.connector;

import be.cetic.tsimulus.config.Configuration;
import com.cloudera.flink.ts.gen.connector.common.TimeSeriesGeneratorSourceOptions;
import com.sun.mail.imap.IMAPFolder;
import io.confluent.avro.random.generator.Generator;
import jakarta.mail.Store;
import org.apache.commons.io.FileUtils;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.data.RowData;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;

public class TimeSeriesGeneratorSource extends RichSourceFunction<RowData> {
    private final TimeSeriesGeneratorSourceOptions options;
    private final List<Column> columns;

    private transient Configuration tsSchemaConfiguration;
    private transient Generator avroGenerator;

    private final long seed = 100L;
    private final long generation = 10000L;

    private transient volatile boolean running = false;

    public TimeSeriesGeneratorSource (
            TimeSeriesGeneratorSourceOptions options,
            List<Column> columns
    ) {
        this.options = options;
        this.columns = columns;
    }

    @Override
    public void run(SourceFunction.SourceContext<RowData> ctx) throws Exception {
        init();
        running = true;

        while (running) {
            // Trigger some TSIMULUS api to generate the records
            TsGenUtil.generateRecord(tsSchemaConfiguration, avroGenerator, columns, ctx);

        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    private void init() throws Exception {
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
