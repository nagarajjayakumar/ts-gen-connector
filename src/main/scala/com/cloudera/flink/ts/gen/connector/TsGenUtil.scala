package com.cloudera.flink.ts.gen.connector

import be.cetic.tsimulus.config.Configuration
import spray.json._

object TsGenUtil {

  def getConfig(content: String) = {
    Configuration(content.parseJson)
  }
}
