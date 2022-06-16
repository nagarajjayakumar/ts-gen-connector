package com.cloudera.flink.ts.gen.connector



import be.cetic.tsimulus._
import be.cetic.tsimulus.config.{Configuration, GeneratorFormat}
import spray.json._
import be.cetic.tsimulus.generators.primary.YearlyGenerator
import com.cloudera.flink.ts.gen.connector.utils.DataGenUtil
import com.github.nscala_time.time.Imports.DateTimeFormat
import io.confluent.avro.random.generator.Generator
import org.apache.avro.generic.GenericRecord
import org.apache.commons.io.IOUtils

import java.nio.charset.StandardCharsets
import java.util.Random


object GenerartorApp extends App {

  val generator = new YearlyGenerator(Some("yearly-generator"), Map(
    2015 -> 42.12,
    2016 -> 13.37,
    2017 -> 6.022)
  )

  println(generator.toJson)

  val seed = 100L
  val content =
    """{
      |  "generators":[
      |    {
      |      "name": "monthly-basis",
      |      "type": "monthly",
      |      "points": {"january": 3.3, "february": 3.7, "march": 6.8, "april": 9.8, "may": 13.6, "june": 16.2,
      |        "july": 18.4, "august": 18, "september": 14.9, "october": 11.1, "november": 6.8, "december": 3.9}
      |    },
      |    {
      |      "name": "daily-basis",
      |      "type": "monthly",
      |      "points": {"january": 3.3, "february": 3.7, "march": 6.8, "april": 9.8, "may": 13.6, "june": 16.2,
      |        "july": 18.4, "august": 18, "september": 14.9, "october": 11.1, "november": 6.8, "december": 3.9}
      |    },
      |    {
      |      "name": "generator1",
      |      "type": "gaussian",
      |      "seed": 42,
      |      "std": 0.5
      |    },
      |    {
      |      "name": "generator2",
      |      "type": "gaussian",
      |      "seed": 11,
      |      "std": 0.9
      |    }
      |  ],
      |  "exported":[
      |    {"name": "temperature", "generator": "generator1", "frequency": 6000},
      |    {"name": "pressure", "generator": "monthly-basis", "frequency": 3000},
      |    {"name": "torque", "generator": "generator2", "frequency": 6000},
      |    {"name": "rpm", "generator": "daily-basis", "frequency": 3000},
      |    {"name": "density", "generator": "generator1", "frequency": 6000},
      |    {"name": "porosity", "generator": "daily-basis", "frequency": 3000},
      |    {"name": "resistivity", "generator": "generator2", "frequency": 6000},
      |    {"name": "crpm", "generator": "monthly-basis", "frequency": 3000},
      |    {"name": "aprs", "generator": "daily-basis", "frequency": 6000},
      |    {"name": "stor", "generator": "monthly-basis", "frequency": 3000},
      |    {"name": "rpm", "generator": "generator1", "frequency": 6000},
      |    {"name": "gamma", "generator": "generator2", "frequency": 1000},
      |    {"name": "Attn", "generator": "generator1", "frequency": 2000}
      |  ],
      |  "from": "2016-01-01 00:00:00.000",
      |  "to": "2017-12-31 23:59:59.999"
      |}""".stripMargin

  val dtf = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss.SSS")

  val classloader = Thread.currentThread.getContextClassLoader
  val inputStream = classloader.getResourceAsStream("transactions.avro")
  val aContent: String = IOUtils.toString(inputStream, StandardCharsets.UTF_8)

  val aGenerator = new Generator.Builder()
    .schemaString(aContent).random(new Random(seed))
    .generation(10000)
    .build()

  val config = Configuration(content.parseJson)

  println("date;series;value")

  Utils.generate(Utils.config2Results(config)) foreach {
    e => {
      val genericRecord = aGenerator.generate().asInstanceOf[GenericRecord]
      genericRecord.put(e._2, e._3)
      println(DataGenUtil.toPrettyFormat(genericRecord.toString))
      println(e._1 + ";" + dtf.print(e._1) + ";" + e._2 + ";" + e._3)
    }
  }

}
