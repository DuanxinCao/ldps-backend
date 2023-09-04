/*
 * erli
 */
package io.glutenproject.ldps.utils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._

object ConverterDataTypeUtils extends Logging {

  def parseFromNativeType(dataNativeType: String): (DataType, Boolean) = {
    dataNativeType match {
      case "UInt8" =>
        (BooleanType, false)
      case "Int8" =>
        (ByteType, false)
      case "Int16" =>
        (ShortType, false)
      case "Int32" =>
        (IntegerType, false)
      case "Int64" =>
        (LongType, false)
      case "Float32" =>
        (FloatType, false)
      case "Float64" =>
        (DoubleType, false)
      case "String" =>
        (StringType, false)
      case "DateTime" =>
        (TimestampType, false)
      case "Date" =>
        (DateType, false)
      case unsupported =>
        throw new UnsupportedOperationException(s"Type $unsupported not supported.")
    }
  }

  def parseFromHive(dataType: DataType): String = {
    dataType match {
      case dataType: BooleanType =>
        "UInt8"
      case dataType: ByteType =>
        "Int8"
      case dataType: ShortType =>
        "Int16"
      case dataType: IntegerType =>
        "Int32"
      case dataType: LongType =>
        "Int64"
      case dataType: FloatType =>
        "Float32"
      case dataType: DoubleType =>
        "Float64"
      case dataType: StringType =>
        "String"
      case dataType: TimestampType =>
        "DateTime"
      case dataType: DateType =>
        "Date"
      case unsupported =>
        throw new UnsupportedOperationException(s"Type $unsupported not supported.")
    }
  }
}
