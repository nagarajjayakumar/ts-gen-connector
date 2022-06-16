package com.cloudera.flink.ts.gen.connector.common;

import lombok.Data;
import lombok.experimental.SuperBuilder;
import org.apache.flink.annotation.Internal;

import java.io.Serializable;

@Internal
@Data
@SuperBuilder(toBuilder = true)
public class TimeSeriesGeneratorSourceOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String tsSchemaLocation;
    private final String tsSchemaFileName;

    private final String avroSchemaLocation;
    private final String avroSchemaFileName;

}
