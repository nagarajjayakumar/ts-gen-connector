package com.cloudera.flink.ts.gen.connector;


import com.cloudera.flink.ts.gen.connector.common.TimeSeriesGeneratorSourceOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import java.util.stream.Collectors;


public class TimeSeriesGeneratorTableSourceFactory implements DynamicTableSourceFactory {

    public static final ConfigOption<String> TS_SCHEMA_LOCATION = ConfigOptions.key("ts_schema_location").stringType().noDefaultValue();
    public static final ConfigOption<String> TS_SCHEMA_FILE_NAME = ConfigOptions.key("ts_schema_file_name").stringType().noDefaultValue();
    public static final ConfigOption<String> AVRO_SCHEMA_LOCATION = ConfigOptions.key("avro_schema_location").stringType().noDefaultValue();
    public static final ConfigOption<String> AVRO_SCHEMA_FILE_NAME = ConfigOptions.key("avro_schema_file_name").stringType().noDefaultValue();

    public static final List<LogicalTypeRoot> SUPPORTED_ROOT_TYPES =
            Arrays.asList(
                    LogicalTypeRoot.DOUBLE,
                    LogicalTypeRoot.FLOAT,
                    LogicalTypeRoot.DECIMAL,
                    LogicalTypeRoot.TINYINT,
                    LogicalTypeRoot.SMALLINT,
                    LogicalTypeRoot.INTEGER,
                    LogicalTypeRoot.BIGINT,
                    LogicalTypeRoot.CHAR,
                    LogicalTypeRoot.VARCHAR,
                    LogicalTypeRoot.BOOLEAN,
                    LogicalTypeRoot.ARRAY,
                    LogicalTypeRoot.MAP,
                    LogicalTypeRoot.ROW,
                    LogicalTypeRoot.MULTISET,
                    LogicalTypeRoot.DATE,
                    LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE,
                    LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                    LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                    LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE);

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

        ResolvedSchema schema = ctx.getCatalogTable().getResolvedSchema();
        List<Column> physicalColumns =
                schema.getColumns().stream()
                        .filter(column -> column.isPhysical())
                        .collect(Collectors.toList());

        int fieldCount = physicalColumns.size();
        String[][] fieldExpressions = new String[fieldCount][];

        for (int i = 0; i < fieldExpressions.length; i++) {
            String fieldName = physicalColumns.get(i).getName();
            DataType dataType = physicalColumns.get(i).getDataType();
            validateDataType(fieldName, dataType);
        }
        return new TimeSeriesGeneratorTableSource(options, physicalColumns);
    }

    private void validateDataType(String fieldName, DataType dataType) {
        if (!SUPPORTED_ROOT_TYPES.contains(dataType.getLogicalType().getTypeRoot())) {
            throw new ValidationException(
                    "Only "
                            + SUPPORTED_ROOT_TYPES
                            + " columns are supported by Faker TableSource. "
                            + fieldName
                            + " is "
                            + dataType.getLogicalType().getTypeRoot()
                            + ".");
        }
    }

}
