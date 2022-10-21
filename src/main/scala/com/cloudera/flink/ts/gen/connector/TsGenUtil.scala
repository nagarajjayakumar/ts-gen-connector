package com.cloudera.flink.ts.gen.connector

import be.cetic.tsimulus.Utils
import be.cetic.tsimulus.config.Configuration
import com.github.nscala_time.time.Imports
import com.github.nscala_time.time.Imports.DateTimeFormat
import io.confluent.avro.random.generator.Generator
import org.apache.avro.generic.GenericRecord
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.table.catalog.Column
import org.apache.flink.table.data.{GenericRowData, RowData, StringData}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.LogicalTypeRoot
import spray.json._

import java.sql.Timestamp
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField
import java.time.{Instant, ZoneId}
import java.util.{Date, Locale}


object TsGenUtil extends Serializable {


  val dtf = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss.SSS")

  val FORMATTER =
    new DateTimeFormatterBuilder()
      // Pattern was taken from java.sql.Timestamp#toString
      .appendPattern("yyyy-MM-dd HH:mm:ss.SSS")
      .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
      .toFormatter(Locale.US);


  def getConfig(content: String) = {
    Configuration(content.parseJson)
  }

  def generateRecord(config: Configuration,
                     aGenerator: Generator,
                     columns: java.util.List[Column],
                     ctx: SourceFunction.SourceContext[RowData]) = {

    import scala.collection.JavaConverters._

    Utils.generate(Utils.config2Results(config)) foreach {
      e => {
        val row = new GenericRowData(columns.size)
        val genericRecord = aGenerator.generate().asInstanceOf[GenericRecord]
        genericRecord.put("ts_timestamp", e._1.toDateTime.getMillis)
        genericRecord.put(e._2, e._3)

        columns.asScala.zipWithIndex.foreach {
          case (column: Column, index: Int) => {
            column.getDataType.getLogicalType.getTypeRoot match {
              case LogicalTypeRoot.CHAR => row.setField(index, StringData.fromString(genericRecord.get(column.getName).asInstanceOf[String]));
              case LogicalTypeRoot.VARCHAR => row.setField(index, StringData.fromString(genericRecord.get(column.getName).asInstanceOf[String]));
              case LogicalTypeRoot.BOOLEAN => row.setField(index, genericRecord.get(column.getName).asInstanceOf[Boolean].booleanValue);

              case LogicalTypeRoot.INTEGER => row.setField(index, genericRecord.get(column.getName).asInstanceOf[Int]);
              case LogicalTypeRoot.BIGINT => row.setField(index, genericRecord.get(column.getName).asInstanceOf[BigInt]);
              case LogicalTypeRoot.FLOAT => row.setField(index, genericRecord.get(column.getName).asInstanceOf[Float]);
              case LogicalTypeRoot.DOUBLE => row.setField(index, genericRecord.get(column.getName).asInstanceOf[Double]);

              case LogicalTypeRoot.DATE => (Date.from(Instant.from(FORMATTER.withZone(ZoneId.systemDefault())
                                                .parse(genericRecord.get(column.getName).asInstanceOf[String]))).getTime
                                             / (86400 * 1000))

              case LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE => Instant.from(FORMATTER.withZone(ZoneId.systemDefault())
                                                                            .parse(genericRecord.get(column.getName).asInstanceOf[String]))

              case LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE => Instant.from(FORMATTER.withZone(ZoneId.systemDefault())
                                                                         .parse(genericRecord.get(column.getName).asInstanceOf[String]))

              case _ => throw new RuntimeException("Unsupported Data Type")

            }
          }
        }
        ctx.collect(row)
      }
    }
  }

  def generateRecord(config: Configuration,
                     aGenerator: Generator,
                     physicalColumnNameToDataTypeMap: java.util.Map[String, DataType],
                     ctx: SourceFunction.SourceContext[RowData]) = {

    import scala.collection.JavaConverters._

    Utils.generate(Utils.config2Results(config)) foreach {
      e => {
        val row = new GenericRowData(physicalColumnNameToDataTypeMap.size)
        val genericRecord = aGenerator.generate.asInstanceOf[GenericRecord]
        genericRecord.put("ts_timestamp", e._1.toDateTime.getMillis)
        genericRecord.put(e._2, e._3)

        physicalColumnNameToDataTypeMap.asScala.zipWithIndex.foreach {
          case ((columnName: String, columnDataType: DataType) , index: Int) => {
            columnDataType.getLogicalType.getTypeRoot match {
              case LogicalTypeRoot.CHAR => row.setField(index, StringData.fromString(genericRecord.get(columnName).asInstanceOf[String]));
              case LogicalTypeRoot.VARCHAR => row.setField(index, StringData.fromString(genericRecord.get(columnName).asInstanceOf[String]));
              case LogicalTypeRoot.BOOLEAN => row.setField(index, genericRecord.get(columnName).asInstanceOf[Boolean].booleanValue);

              case LogicalTypeRoot.INTEGER => row.setField(index, genericRecord.get(columnName).asInstanceOf[Int]);
              case LogicalTypeRoot.BIGINT => row.setField(index, genericRecord.get(columnName).asInstanceOf[java.lang.Long]);
              case LogicalTypeRoot.FLOAT => row.setField(index, genericRecord.get(columnName).asInstanceOf[Float]);
              case LogicalTypeRoot.DOUBLE => row.setField(index, genericRecord.get(columnName).asInstanceOf[Double]);

              case LogicalTypeRoot.DATE => (Date.from(Instant.from(FORMATTER.withZone(ZoneId.systemDefault())
                .parse(genericRecord.get(columnName).asInstanceOf[String]))).getTime
                / (86400 * 1000))

              case LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE => Instant.from(FORMATTER.withZone(ZoneId.systemDefault())
                .parse(genericRecord.get(columnName).asInstanceOf[String]))

              case LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE => Instant.from(FORMATTER.withZone(ZoneId.systemDefault())
                .parse(genericRecord.get(columnName).asInstanceOf[String]))

              case _ => throw new RuntimeException("Unsupported Data Type")

            }
          }
        }
        ctx.collect(row)
      }
    }
  }
}
