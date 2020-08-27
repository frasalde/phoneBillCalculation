package com.frasalde.pbc

object Utils {

  def enrichException(e: Exception, detail: String): EnrichedException = {
    val originalType = e.getClass.getName
    val originalMessage = e.getMessage
    EnrichedException(s"type=[$originalType], message=[$originalMessage], detail=[$detail]", e)
  }

}
