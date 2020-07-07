package com.santander.mdc.modelodecontrol

case class ControlRow(ctvr_id_control: String,
                         ctvr_origen: String,
                         ctvr_tabla: String,
                         ctvr_variable: String,
                         ctvr_secuencia: Int,
                         ctvr_tipo_registro: String,
                         ctvr_clave_cruce: Boolean,
                         ctvr_transformacion: String,
                         ctvr_fecha_hora_alta: String
                        )
