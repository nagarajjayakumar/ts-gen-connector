package com.cloudera.flink.ts.gen.connector;

import com.cloudera.flink.ts.gen.connector.common.TimeSeriesGeneratorSourceOptions;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;

import java.util.List;

public class TimeSeriesGeneratorTableSource implements ScanTableSource {

    private final TimeSeriesGeneratorSourceOptions options;
    private final List<Column> columns;

    public TimeSeriesGeneratorTableSource(
            TimeSeriesGeneratorSourceOptions options,
            List<Column> columns
    ) {
        this.options = options;
        this.columns = columns;
    }


    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext ctx) {
        boolean bounded = false;
        final TimeSeriesGeneratorSource source = new TimeSeriesGeneratorSource(options, columns);
        return SourceFunctionProvider.of(source, bounded);
    }

    @Override
    public DynamicTableSource copy() {
        return new TimeSeriesGeneratorTableSource(options, columns);
    }

    @Override
    public String asSummaryString() {
        return "TimeSeries Generator Table Source";
    }
}