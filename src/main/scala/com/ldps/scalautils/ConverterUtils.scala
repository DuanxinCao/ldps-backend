package com.ldps.scalautils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.{ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, NullType, ShortType, StringType}

object ConverterUtils extends Logging {

  def parseFromNativeType(dataNativeType: String): (DataType, Boolean) = {
    dataNativeType match {
      case "UInt64" =>
        (LongType,false)
      case "String" =>
        (StringType,false)
      case "Int8" =>
        (ByteType,false)
      case "Int16" =>
        (ShortType,false)
      case "Int32" =>
        (IntegerType,false)
      case "Float32" =>
        (FloatType,false)
      case "Float64"=>
        (DoubleType,false)
      case "nullType" =>
        (NullType, true)
      case unsupported =>
        throw new UnsupportedOperationException(s"Type $unsupported not supported.")
    }
  }
}
