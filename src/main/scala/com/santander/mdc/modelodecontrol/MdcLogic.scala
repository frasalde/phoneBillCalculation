package com.santander.mdc.modelodecontrol

import com.santander.mdc.Utils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}

case class ControlRow(ctvr_IdControl: String, ctvr_Origen: String, ctvr_Tabla: String, ctvr_Variable: String, ctvr_Secuencia: Int, ctvr_Transformacion: String, ctvr_IdRegistro: String, ctvr_ClaveCruce: String)


class MdcLogic(implicit spark: SparkSession) {

  import MdcLogic._

  var loadedTables: Map[String, DataFrame] = Map()

  def run(): Unit = {
    import spark.implicits._

    //Lectura de tablas de control
    val condicionControlesActivos: Column = col("ctrl_Estado").equalTo("Activo").and(col("ctrl_Fecha_Baja").isNull.or(col("ctrl_Fecha_Baja").equalTo(lit(""))))
    val controlesActivos: Array[Row] = Utils.readTableByFilter(TABLA_CONTROLES, condicionControlesActivos)
      .select("ctrl_IdControl").collect()
    val controlVariable: Dataset[ControlRow] = Utils.readTableByFilter(TABLA_CONTROL_VARIABLE, col("idControl").isin(controlesActivos)).as[ControlRow]
    val parametria: DataFrame = Utils.readTableByFilter(TABLA_PARAMETRIA, col("idControl").isin(controlesActivos))

    //todo minusculas
   //case class ControlRow(ctvr_IdControl: String, ctvr_Origen: String, ctvr_Tabla: String, ctvr_Variable: String, ctvr_Secuencia: Int, ctvr_Transformacion: String, ctvr_IdRegistro: String, ctvr_ClaveCruce: String)


    val controlRows: Array[ControlRow] = controlVariable.collect()

/*    val controlRows: Array[ControlRow] = controlVariable.collect.map(r => ControlRow(
      r.getAs[String]("ctvr_IdControl"),
      r.getAs[String]("ctvr_Origen"),
      r.getAs[String]("ctvr_Tabla"),
      r.getAs[String]("ctvr_Variable"),
      r.getAs[Int]("ctvr_Secuencia"),
      r.getAs[String]("ctvr_Transformacion"),
      r.getAs[String]("ctvr_IdRegistro"),
      r.getAs[String]("ctvr_ClaveCruce")

    ))*/

    val controlRowsById: Map[String, Array[ControlRow]] = controlRows.groupBy(_.ctvr_IdControl)

    //////////////////////////////
    // Operación de concatenado //
    /////////////////////////////
    val resultDataFrames = controlRowsById.map {

      case (controlId: String, rows: Array[ControlRow]) =>

        val usefulTables: Array[String] = rows.map(c => s"${c.ctvr_Origen}.${c.ctvr_Tabla}").distinct
        val usefulDataFrames: Array[DataFrame] = usefulTables.map(getTable)

        //Join de todas las tablas necesarias para las columnas de este control_id
        //Aún está por definir cómo se relacionan entre sí
        //De momento supongo que todas se relacionan por el campo "key"
        //val dfAllColumns: DataFrame = usefulDataFrames.reduceLeft((df1, df2) => df1.join(df2, Seq("key"), "full"))
        val dfAllColumns: DataFrame = usefulDataFrames.head

        val columnsToConcatI: Array[org.apache.spark.sql.Column] = rows.filter(_.ctvr_IdRegistro == "Identificador").sortBy(_.ctvr_Secuencia).map { r =>
          getTable(s"${r.ctvr_Origen}.${r.ctvr_Tabla}").col(r.ctvr_Variable)
          //TODO aplicar transformaciones
        }

        val columnsToConcatV: Array[org.apache.spark.sql.Column] = rows.filter(_.ctvr_IdRegistro == "Valor").sortBy(_.ctvr_Secuencia).map { r =>
          getTable(s"${r.ctvr_Origen}.${r.ctvr_Tabla}").col(r.ctvr_Variable)
          //TODO aplicar transformaciones
        }


        val dfAllColumnsFix: DataFrame = Utils.fillNullOrEmpty2(dfAllColumns, columnsToConcatV.map(c => c.toString()))

        val dfConcatenated = dfAllColumns
          .withColumn("rslt_control_id", lit(controlId))
          .withColumn("rslt_id_registro", concat_ws(" | ", columnsToConcatI: _*))
          .withColumn("rslt_valor_control", concat_ws(" | ", columnsToConcatV: _*))
          .select("rslt_control_id", "rslt_id_registro", "rslt_valor_control")

        dfConcatenated.show
        dfConcatenated
    }

    val dfAControlar = resultDataFrames.reduceLeft((df1, df2) => df1.union(df2))
    dfAControlar.show

    //Cruce con la tabla de parametria
    //todo condición también con controlId
    //todo cuando ctvl_status = null -> ctvl_status = pd
    val result: DataFrame = dfAControlar.join(parametria,  dfAControlar.col("rslt_valor_control") === parametria.col("rslt_valor_control"), "left")

    //Escritura
    Utils.writeResult(result, "bbddFinal", "resultados")
  }

  /**
   * If we have not read this table before, it registers it and returns the dataframe.
   * If we already have it registered, we simply return the dataframe.
   *
   * @param name
   * @return DF with the table
   */
  def getTable(name: String): DataFrame = {
    if(loadedTables.contains(name)){
      loadedTables(name)
    }else{
      val table: DataFrame = Utils.readTable(name)
      loadedTables += name -> table
      table
    }
  }

}


object MdcLogic {

  val TABLA_CONTROLES: String = "controles"
  val TABLA_CONTROL_VARIABLE: String = "par_valores_control"
  val TABLA_PARAMETRIA: String = ""

}
