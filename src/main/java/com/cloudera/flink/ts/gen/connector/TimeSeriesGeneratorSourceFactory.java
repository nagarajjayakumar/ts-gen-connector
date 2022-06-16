package com.cloudera.flink.ts.gen.connector;


import com.cloudera.flink.ts.gen.connector.common.TimeSeriesGeneratorSourceOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


public class TimeSeriesGeneratorSourceFactory implements DynamicTableSourceFactory {

    public static final ConfigOption<String> TS_SCHEMA_LOCATION = ConfigOptions.key("ts_schema_location").stringType().noDefaultValue();
    public static final ConfigOption<String> TS_SCHEMA_FILE_NAME = ConfigOptions.key("ts_schema_file_name").stringType().noDefaultValue();
    public static final ConfigOption<String> AVRO_SCHEMA_LOCATION = ConfigOptions.key("avro_schema_location").stringType().noDefaultValue();
    public static final ConfigOption<String> AVRO_SCHEMA_FILE_NAME = ConfigOptions.key("avro_schema_file_name").stringType().noDefaultValue();

    @Override
    public String factoryIdentifier() {
        return "ts_gen";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(TS_SCHEMA_LOCATION);
        options.add(TS_SCHEMA_FILE_NAME);
        options.add(AVRO_SCHEMA_LOCATION);
        options.add(AVRO_SCHEMA_FILE_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context ctx) {
        final FactoryUtil.TableFactoryHelper factoryHelper = FactoryUtil.createTableFactoryHelper(this, ctx);
        factoryHelper.validate();

        final TimeSeriesGeneratorSourceOptions options = TimeSeriesGeneratorSourceOptions.builder()
                .tsSchemaLocation(factoryHelper.getOptions().get(TS_SCHEMA_LOCATION))
                .tsSchemaFileName(factoryHelper.getOptions().get(TS_SCHEMA_FILE_NAME))
                .avroSchemaLocation(factoryHelper.getOptions().get(AVRO_SCHEMA_LOCATION))
                .avroSchemaFileName(factoryHelper.getOptions().get(AVRO_SCHEMA_FILE_NAME))
                .build();

        final List<Column> columns = ctx.getCatalogTable().getResolvedSchema().getColumns();

        return new TimeSeriesGeneratorTableSource(options, columns);
    }
}
