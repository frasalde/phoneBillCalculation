package com.frasalde.pbc

import com.frasalde.pbc.phonebillcalc.PbcLogic
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class PbcTests extends FunSuite with BeforeAndAfterEach {

    implicit var sparkSession : SparkSession = _
    override def beforeEach() {
    sparkSession = SparkSession.builder().appName("MDC testings")
        .master("local")
        .config("", "")
        .enableHiveSupport()
        .getOrCreate()
  }


  test("Test 1 - Bill calculation") {

  val pbcLogic = new PbcLogic()
  val result = pbcLogic.run()
    assert(result == 900)
  }

  test("Test 2 - Intercept Exception") {

    val pbcLogic = new PbcLogic()
    val callLogs: String = ""

    intercept[Exception] {
      pbcLogic.phonebillcalculation(callLogs)
    }
  }

}
