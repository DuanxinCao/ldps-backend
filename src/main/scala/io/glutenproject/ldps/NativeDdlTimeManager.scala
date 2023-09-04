package io.glutenproject.ldps

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier

import scala.collection.mutable.Map

object NativeDdlTimeManager extends Logging {
  var DDLTime: Map[TableIdentifier, String] = Map.empty

  def checkDDLTime(identifier: TableIdentifier, lastDDLTime: String): Boolean = {
    if (DDLTime.contains(identifier)) {
      if (lastDDLTime.toLong <= DDLTime.getOrElse(identifier, "0").toLong) {
        true
      } else {
        false
      }
    } else {
      false
    }
  }

  def setDDLTime(identifier: TableIdentifier, lastDDLTime: String): Unit = {
    if (DDLTime.contains(identifier)) {
      DDLTime(identifier) = lastDDLTime
    } else {
      DDLTime.put(identifier, lastDDLTime)
    }
  }

}
