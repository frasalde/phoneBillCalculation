package com.santander.mdc

import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

object Utils {

  def enrichException(e: Exception, detail: String): EnrichedException = {
    val originalType = e.getClass.getName
    val originalMessage = e.getMessage
    EnrichedException(s"type=[$originalType], message=[$originalMessage], detail=[$detail]", e)
  }

  /**
   * Reading a table
   * Always specify bbdd
   * @param table
   * @return DF with the table
   */
  def readTable(table: String)(implicit spark: SparkSession): DataFrame = {
    try {
      spark.sql(s"SELECT * FROM ${table}")
    } catch {
      case e: Exception => throw enrichException(e, table)
    }
  }

  /**
   * Reading a table for a specific date
   *
   * @param dateField
   * @param dateValue
   * @param table
   * @return filtered table
   */
  def readTableByDate(
                       dateField: String,
                       dateValue: String,
                       table: String)(implicit spark: SparkSession): DataFrame = {
    readTable(table)
      .where(col(dateField) === dateValue)
  }

  /**
   * Reading a table with a filter
   *
   * @param table
   * @param filter
   * @return filtered table
   */
  def readTableByFilter(
                         table: String,
                         filter: Column)(implicit spark: SparkSession): DataFrame = {
    readTable(table)
      .where(filter)
  }


  /**
   * Replace null Strings with (nulo) and empty strings with (vacío)
   *
   * @param df
   * @param columnToApply
   * @return DF with new values
   */
  def fillNullOrEmpty(df: DataFrame,
                      columnToApply: String): DataFrame = {

    df.withColumn(columnToApply, when(col(columnToApply).isNull, lit("(nulo)"))
      .when(col(columnToApply).equalTo(lit("")), lit("(vacío)"))
      .otherwise(col(columnToApply))
    )
  }

  /**
   * Replace null Strings with (nulo) and empty strings with (vacío)
   *
   * @param df
   * @param columnsToApply
   * @return DF with new values
   */
  def fillNullOrEmpty2(df: DataFrame,
                      columnsToApply: Seq[String]): DataFrame = {

    columnsToApply.foldLeft(df)(
      (df, c) => df.withColumn(c, when(col(c).isNull, lit("(nulo)")).when(col(c).equalTo(""), lit("(vacío)")).otherwise(col(c)))
    )
  }

  def writeResult(result: DataFrame, database: String, tableName: String )(implicit spark: SparkSession): Unit = {

    result
      .write.mode(SaveMode.Overwrite)
      .insertInto(s"${database}.${tableName}")

  }





}
