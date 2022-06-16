package com.cloudera.flink.ts.gen.connector;

import com.cloudera.flink.ts.gen.connector.common.TimeSeriesGeneratorSourceOptions;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.Map;

public class TimeSeriesGeneratorTableSource implements ScanTableSource {

    private final TimeSeriesGeneratorSourceOptions options;
    private final Map<String, DataType> physicalColumnNameToDataTypeMap;

    public TimeSeriesGeneratorTableSource(
            TimeSeriesGeneratorSourceOptions options,
            Map<String, DataType> physicalColumnNameToDataTypeMap
    ) {
        this.options = options;
        this.physicalColumnNameToDataTypeMap = physicalColumnNameToDataTypeMap;
    }


    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext ctx) {
        boolean bounded = false;
        final TimeSeriesGeneratorSource source = new TimeSeriesGeneratorSource(options, physicalColumnNameToDataTypeMap);
        return SourceFunctionProvider.of(source, bounded);
    }

    @Override
    public DynamicTableSource copy() {
        return new TimeSeriesGeneratorTableSource(options, physicalColumnNameToDataTypeMap);
    }

    @Override
    public String asSummaryString() {
        return "TimeSeries Generator Table Source";
    }
}