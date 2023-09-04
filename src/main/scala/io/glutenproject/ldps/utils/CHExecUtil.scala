package io.glutenproject.ldps.utils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.DataType

object CHExecUtil extends Logging {

  def inferSparkDataType(nativeType: String): DataType = {
    val (datatype, nullable) =
      ConverterDataTypeUtils.parseFromNativeType(nativeType)
    datatype
  }
}