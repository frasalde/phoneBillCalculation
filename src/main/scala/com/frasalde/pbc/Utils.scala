package com.frasalde.pbc

import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

object Utils {

  def enrichException(e: Exception, detail: String): EnrichedException = {
    val originalType = e.getClass.getName
    val originalMessage = e.getMessage
    EnrichedException(s"type=[$originalType], message=[$originalMessage], detail=[$detail]", e)
  }

}
