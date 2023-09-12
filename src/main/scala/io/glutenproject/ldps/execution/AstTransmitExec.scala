package io.glutenproject.ldps.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{InternalRow}
import org.apache.spark.sql.catalyst.analysis.CastSupport
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.hive.HiveExternalCatalog.HIVE_GENERATED_TABLE_PROPERTIES
import org.apache.spark.sql.vectorized.ColumnarBatch
import io.glutenproject.ldps.backend.{CHNativeExpressionEvaluator, GeneralInIterator}
import io.glutenproject.ldps.NativeDdlTimeManager
import io.glutenproject.ldps.utils.ConverterDataTypeUtils

import java.util

case class AstTransmitExec(
                            sqlText: String,
                            var catalogTable: CatalogTable,
                            var outputSchema: Seq[Attribute]) extends LeafExecNode with CastSupport {
  var canConvert: Boolean = true

  override def supportsColumnar: Boolean = true

  // TODO configure it
  //  val hiveMetaStoreUri: String = "thrift://ld-bp147mer22hw5ob6s-proxy-ldps-hms.lindorm.aliyuncs.com:9083"

  def setCanConvert(flag: Boolean): Unit = {
    canConvert = flag
  }

  def setCatalogTable(catalog: CatalogTable): Unit = {
    catalogTable = catalog
  }

  def getSql(): String = {
    sqlText
  }

  def setOutputSchema(outputSchema: Seq[Attribute]): Unit = {
    this.outputSchema = outputSchema
  }

  def getEngineMergeTree(): String = {
    s"MergeTree"
  }

  def getEngineHive(): String = {
    s"HIVE('${sparkContext.getConf.get("spark.hadoop.hive.metastore.uris")}', '${catalogTable.database}', '${catalogTable.identifier.table}')"
  }

  def getCreateTabelSql(): String = {
    s"CREATE TABLE IF NOT EXISTS ${catalogTable.database}.${catalogTable.identifier.table} (" +
      catalogTable.schema.map { field =>
        s"${field.name} ${ConverterDataTypeUtils.parseFromHive(field.dataType)}"
      }.mkString(", ") +
      s") ENGINE = ${getEngineHive()} " +
      " PARTITION BY (" +
      catalogTable.partitionColumnNames.mkString(", ") +
      ") PRIMARY KEY ()"
  }

  def getDropTableSql(): String = {
    s"DROP TABLE IF EXISTS ${catalogTable.database}.${catalogTable.identifier.table}"
  }

  private def doExecuteColumnarInternal(): RDD[ColumnarBatch] = {
    System.loadLibrary("ldpsbackend")

    val startTime = System.currentTimeMillis();
    val lastDDLTime: String = catalogTable.properties.getOrElse(HIVE_GENERATED_TABLE_PROPERTIES.mkString, "0")

    // test for MEMORY ENGINE
    if (!NativeDdlTimeManager.checkDDLTime(catalogTable.identifier, lastDDLTime)) {
      val chEval = new CHNativeExpressionEvaluator
      val iterList = new util.ArrayList[GeneralInIterator]
      // drop table
      val dropTableSQL = getDropTableSql()
      println(s"cdx: drop table sql : ${dropTableSQL}")
      chEval.createKernelWithBatchIterator(sqlText, iterList, null)

      // create table
      // ddl scahema partial example :
      // ENGINE = HIVE('thrift: //ld-bp147mer22hw5ob6s-proxy-ldps-hms.lindorm.aliyuncs.com:9083', 'clickbench', 'hitsck')
      // PARTITION BY EventDate;
      val createTableSQL = getCreateTabelSql()
      println(s"cdx: create table sql : ${createTableSQL}")
      chEval.createKernelWithBatchIterator(createTableSQL, iterList, null)
      NativeDdlTimeManager.setDDLTime(catalogTable.identifier, lastDDLTime)
    }

    var mergedRDD: RDD[ColumnarBatch] = sparkContext.emptyRDD[ColumnarBatch]
    val costTime = System.currentTimeMillis() - startTime;
    logError(s"[$sqlText] clickhouse command cost ${costTime}ms")
    mergedRDD
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
//    System.loadLibrary("ldpsbackend")

    val startTime = System.currentTimeMillis();
    val lastDDLTime: String = catalogTable.properties.getOrElse(HIVE_GENERATED_TABLE_PROPERTIES.mkString, "0")

    if (!NativeDdlTimeManager.checkDDLTime(catalogTable.identifier, lastDDLTime)) {
      val chEval = new CHNativeExpressionEvaluator
      val iterList = new util.ArrayList[GeneralInIterator]
      // drop table
      val dropTableSQL = getDropTableSql()
      println(s"cdx: drop table sql : ${dropTableSQL}")
      chEval.createKernelWithBatchIterator(dropTableSQL, iterList, null)

      // create table
      // ddl scahema partial example :
      // ENGINE = HIVE('thrift: //ld-bp147mer22hw5ob6s-proxy-ldps-hms.lindorm.aliyuncs.com:9083', 'clickbench', 'hitsck')
      // PARTITION BY EventDate;
      val createTableSQL = getCreateTabelSql()
      println(s"cdx: create table sql : ${createTableSQL}")
      chEval.createKernelWithBatchIterator(createTableSQL, iterList, null)
      NativeDdlTimeManager.setDDLTime(catalogTable.identifier, lastDDLTime)
    }

    // TODO return iterator
    // dml sql
    val chEval = new CHNativeExpressionEvaluator
    val iterList = new util.ArrayList[GeneralInIterator]
    val it = chEval.createKernelWithBatchIterator(sqlText, iterList, null)
    var mergedRDD: RDD[ColumnarBatch] = sparkContext.emptyRDD[ColumnarBatch]
    while (it.hasNext) {
      //      val columnarBatch: ColumnarBatch = it.next
      //      val internalRowIterator = columnarBatch.rowIterator
      //      while (internalRowIterator.hasNext) {
      //        val next = internalRowIterator.next
      //        System.out.println(next.getInt(0));
      //        System.out.println(next.getString(1));
      //      }
      mergedRDD = sparkContext.union(Seq(mergedRDD, sparkContext.parallelize(Seq(it.next()))))
    }
    val costTime = System.currentTimeMillis() - startTime;
    logError(s"[$sqlText] clickhouse command cost ${costTime}ms")
    mergedRDD
  }

  /**
   * Produces the result of the query as an `RDD[InternalRow]`
   *
   * Overridden by concrete implementations of SparkPlan.
   */
  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException

  override def output: Seq[Attribute] = {
    outputSchema
  }

}
