package com.frasalde.pbc.phonebillcalc

/**
 * @author ${user.name}
 */
import org.apache.spark.sql.SparkSession

/**
 * @author ${user.name}
 */
object PbcDriver {

  def main(args : Array[String]) {

    // Create Spark Session
    implicit val spark = SparkSession
      .builder()
      .appName("mdc-engine")
      .getOrCreate()

    // instantiate logic module & run
    val logic: PbcLogic = new PbcLogic
    logic.run

    spark.stop()

  }

}
