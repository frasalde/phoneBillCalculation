package com.frasalde.pbc.phonebillcalc

import com.frasalde.pbc.Utils
import org.apache.spark.sql._

class PbcLogic(implicit spark: SparkSession) {

  /**
    * run method
   */
  def run(): Int = {

    val callLogs: String = "00:01:07,400-234-090\n00:05:01,701-080-080\n00:05:00,400-234-090\n"
    phonebillcalculation(callLogs)
  }

  /**
     * Phone bill calculation method
     * @param callLogs call logs string
     * @return bill
   */
  def phonebillcalculation (callLogs: String): Int = {

    // Check logs size
    val LogSize: Int = callLogs.split("\n").size
    if (callLogs == "" || LogSize > 100) {
      throw Utils.enrichException(new Exception, "Log size out of range")
    }

    val phoneBills: Map[String, Array[String]] = callLogs.split("\n").groupBy(_.substring(9,20))

    val resultsMap: Map[String, Int] = phoneBills.map {

      case (phoneNumber: String, logs: Array[String]) =>

        val allCents: Array[Int] = logs.map {
          l =>
            val totalHours: Int = l.substring(0, 2).toInt
            val totalMinutes: Int = l.substring(3, 5).toInt
            val totalSeconds: Int = l.substring(6, 8).toInt

            val totalCents: Int = if (totalMinutes < 5) {
              ((totalHours * 360) + (totalMinutes * 60) + totalSeconds) * 3
            } else {
              if (totalSeconds > 0) {
                ((totalHours * 60) + totalMinutes + 1) * 150
              } else {
                ((totalHours * 60) + totalMinutes) * 150
              }
            }
            totalCents
        }
        (phoneNumber, allCents.sum)
    }

    val resultTail: List[(String, Int)] = resultsMap.toList.sortBy(r => (r._1, r._2)).tail
    resultTail.map(_._2).sum
  }
  run()

}