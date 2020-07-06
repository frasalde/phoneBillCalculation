package com.santander.mdc.modelodecontrol

/**
 * @author ${user.name}
 */
import org.apache.spark.sql.SparkSession

/**
 * @author ${user.name}
 */
object MdcDriver {

  def main(args : Array[String]) {

    // Create Spark Session
    implicit val spark = SparkSession
      .builder()
      .appName("mdc-engine")
      .getOrCreate()

    // instantiate logic module & run
    val logic: MdcLogic = new MdcLogic
    logic.run

    spark.stop()

  }

}
