package com.cloudera.flink.ts.gen.connector;

import be.cetic.tsimulus.Utils;
import be.cetic.tsimulus.config.Configuration;
import io.confluent.avro.random.generator.Generator;
import com.cloudera.flink.ts.gen.connector.common.TimeSeriesGeneratorSourceOptions;
import com.sun.mail.imap.IMAPFolder;
import jakarta.mail.*;
import jakarta.mail.event.MessageCountAdapter;
import jakarta.mail.event.MessageCountEvent;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;

public class TimeSeriesGeneratorSource extends RichSourceFunction<RowData> {
    private final TimeSeriesGeneratorSourceOptions options;
    private final List<Column> columns;

    private transient Configuration tsSchemaConfiguration;
    private transient Generator avroGenerator;

    private final long seed = 100L;
    private final long generation = 10000L;
    private transient Store store;
    private transient IMAPFolder folder;

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
        connect();
        running = true;

        folder.addMessageCountListener(new MessageCountAdapter() {
            @Override
            public void messagesAdded(MessageCountEvent e) {
                collectMessages(ctx, e.getMessages());
            }
        });

        while (running) {
            // Trigger some IMAP request to force the server to send a notification
            TsGenUtil.generateRecord(tsSchemaConfiguration, avroGenerator, columns, ctx);

        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void close() throws Exception {
        if (folder != null) {
            folder.close();
        }

        if (store != null) {
            store.close();
        }
    }

    private void collectMessages(SourceFunction.SourceContext<RowData> ctx, Message[] messages) {
        for (Message message : messages) {
            try {
                collectMessage(ctx, message);
            } catch (MessagingException ignored) {}
        }
    }

    private void collectMessage(SourceFunction.SourceContext<RowData> ctx, Message message)
            throws MessagingException {
        final GenericRowData row = new GenericRowData(columns.size());
        final GenericRecord genericRecord = (GenericRecord) avroGenerator.generate();

        Column column = null;
        for (int i = 0; i < columns.size(); i++) {
            column = columns.get(i);
            row.setField(i, genericRecord.get(column.getName()));
            switch (column.getName()) {
                case "SUBJECT":
                    row.setField(i, StringData.fromString(message.getSubject()));
                    break;
                case "SENT":
                    row.setField(i, TimestampData.fromInstant(message.getSentDate().toInstant()));
                    break;
                case "RECEIVED":
                    row.setField(i, TimestampData.fromInstant(message.getReceivedDate().toInstant()));
                    break;
                // ...
            }
        }

        ctx.collect(row);
    }

    private void connect() throws Exception {
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
