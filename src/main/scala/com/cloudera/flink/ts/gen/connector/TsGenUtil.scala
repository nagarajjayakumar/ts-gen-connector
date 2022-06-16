package com.cloudera.flink.ts.gen.connector

import be.cetic.tsimulus.Utils
import be.cetic.tsimulus.config.Configuration
import com.cloudera.flink.ts.gen.connector.utils.DataGenUtil
import com.github.nscala_time.time.Imports.DateTimeFormat
import org.apache.avro.generic.GenericRecord
import spray.json._
import io.confluent.avro.random.generator.Generator
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.DataTypes.BIGINT
import org.apache.flink.table.catalog.Column
import org.apache.flink.table.data.{GenericRowData, RowData}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.{LogicalType, LogicalTypeRoot}

object TsGenUtil {

  val dtf = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss.SSS")

  def getConfig(content: String) = {
    Configuration(content.parseJson)
  }

  def generateRecord (config: Configuration,
                      aGenerator: Generator,
                      columns: java.util.List[Column],
                      ctx: SourceFunction.SourceContext[RowData]) = {

    import scala.collection.JavaConverters._

    Utils.generate(Utils.config2Results(config)) foreach {
      e => {
        val row = new GenericRowData(columns.size)
        val genericRecord = aGenerator.generate().asInstanceOf[GenericRecord]
        columns.asScala.zipWithIndex.foreach {
          case (column: Column, index: Int) => {
              column.getDataType.getLogicalType.getTypeRoot match {
                case LogicalTypeRoot.INTEGER => row.setField(index, genericRecord.get(column.getName).asInstanceOf[Int]);
              }
          }
        }
        genericRecord.put(e._2, e._3)
        println(DataGenUtil.toPrettyFormat(genericRecord.toString))
        println(e._1 + ";" + dtf.print(e._1) + ";" + e._2 + ";" + e._3)
        ctx.collect(row)
      }
    }
  }
}
