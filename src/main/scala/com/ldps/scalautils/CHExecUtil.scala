package com.ldps.scalautils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.DataType

object CHExecUtil extends Logging {

  def inferSparkDataType(nativeType: String): DataType = {
    val (datatype, nullable) =
      ConverterUtils.parseFromNativeType(nativeType)
    datatype
  }
}