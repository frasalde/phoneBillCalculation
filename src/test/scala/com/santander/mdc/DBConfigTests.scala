package com.santander.mdc


//import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class DBConfigTests extends FunSuite with BeforeAndAfterEach {

    implicit var sparkSession : SparkSession = _
    override def beforeEach() {
    sparkSession = SparkSession.builder().appName("MDC testings")
        .master("local")
        .config("", "")
        .enableHiveSupport()
        .getOrCreate()

      sparkSession.sql("CREATE DATABASE IF NOT EXISTS bu_fran")
/*      sparkSession.sql("CREATE TABLE IF NOT EXISTS bu_fran.test (nombre String, apellido String, edad int)" +
        " row format delimited " +
        "fields terminated by ';'" +
        " STORED AS TEXTFILE")*/

/*
      val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    sparkSession = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config("hadoop.home.dir", "C:\\hadoop-2.6.0")
      .config("hive.exec.scratchdir", "C:\\Users\\ja.rodriguez\\tmp\\hive")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
*/

   /*   val MVYT24BE_sample = sparkSession.read.format("csv")
        .option("header", "true")
        .option("delimiter", ";")
        .load("src/test/resources/MVYT24BE_sample.csv")
      MVYT24BE_sample.write.saveAsTable("bu_fran.MVYT24BE")*/

/*      val testFile = sparkSession.read.format("csv")
        .option("header", "true")
        .option("delimiter", ";")
        .load("src/test/resources/test.csv")

      testFile.show()
      testFile.write.saveAsTable("bu_fran.test")*/

      val testFile = sparkSession.read.format("csv")
        .option("header", "true")
        .option("delimiter", ";")
        .load("src/test/resources/MVYT24BE_sample.csv")

      testFile.show()
      testFile.write.saveAsTable("bu_fran.MVYT24BE")

   /*   testFile.createOrReplaceTempView("temporalTable")
      sparkSession.sql(s"INSERT overwrite TABLE bu_fran.test SELECT * FROM temporalTable")*/
  }

  override def afterEach() {

    sparkSession.sql("DROP TABLE bu_fran.MVYT24BE")
    sparkSession.sql("DROP DATABASE bu_fran")

  }


  test("Test 1") {
    val MVYT24BE_sample = Utils.readTable("bu_fran.MVYT24BE")
    MVYT24BE_sample.show(false)
  }






}
