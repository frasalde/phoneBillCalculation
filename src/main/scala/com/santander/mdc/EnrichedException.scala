package com.santander.mdc

final case class EnrichedException(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause)
