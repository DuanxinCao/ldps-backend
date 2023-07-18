package com.ldps.scalautils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.{DataType, LongType, NullType, StringType}

object ConverterUtils extends Logging {

  def parseFromNativeType(dataNativeType: String): (DataType, Boolean) = {
    dataNativeType match {
      case "UInt64" =>
        (LongType,false)
      case "String" =>
        (StringType,false)
      case "nullType" =>
        (NullType, true)
      case unsupported =>
        throw new UnsupportedOperationException(s"Type $unsupported not supported.")
    }
  }
}
