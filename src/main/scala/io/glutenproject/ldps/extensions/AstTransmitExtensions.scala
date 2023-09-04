package io.glutenproject.ldps.extensions

import io.glutenproject.ldps.utils.ConverterDataTypeUtils
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{SQLConfHelper, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Count, Max, Min, Sum}
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, And, Attribute, AttributeReference, CaseWhen, Cast, Contains, Divide, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, Hour, In, InSet, IsNotNull, IsNull, Length, LessThan, LessThanOrEqual, Literal, Minute, Month, Multiply, Not, Or, RegExpReplace, Second, SortOrder, Subtract, TruncTimestamp, Year}
import org.apache.spark.sql.catalyst.optimizer.JoinSelectionHelper
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, AsOfJoin, CTERelationDef, CTERelationRef, CollectMetrics, Deduplicate, Distinct, DomainJoin, Expand, Filter, Generate, GlobalLimit, InsertIntoDir, Intersect, Join, LateralJoin, LocalLimit, LogicalPlan, OneRowRelation, Pivot, Project, Range, RebalancePartitions, Repartition, RepartitionByExpression, ReturnAnswer, Sample, Sort, Subquery, Tail, Union, UnresolvedWith, View, Window, WithCTE, WithWindowDefinition}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.LogicalQueryStage
import org.apache.spark.sql.execution.command.CreateDataSourceTableCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog
import org.apache.spark.sql.hive.HiveExternalCatalog.DATASOURCE_PROVIDER
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import io.glutenproject.ldps.execution.AstTransmitExec



case class AstTransmitOverrides(session: SparkSession) extends Strategy with
  JoinSelectionHelper with SQLConfHelper {
  def checkTableSchema(identifier: TableIdentifier): (Boolean,CatalogTable) = {
    val catalogTable: CatalogTable = session.sessionState.catalog.getTableCatalog(identifier)

    // check datasource
    if (session.sparkContext.getConf.get("spark.sql.catalogImplementation").equals("hive") &&
      session.sessionState.catalogManager.currentCatalog.isInstanceOf[V2SessionCatalog]) {
      // check data source provider
      if (catalogTable.properties.get(DATASOURCE_PROVIDER) match {
        case None =>
          false
        case Some(provider) =>
          if (provider.equals("parquet") || provider.equals("orc")) {
            true
          } else {
            false
          }
      }) {
        // check schema
        if (catalogTable.schema.forall { field =>
          try {
            ConverterDataTypeUtils.parseFromHive(field.dataType)
            true
          } catch {
            case _:Throwable =>
              false
          }
        }) {
          (true,catalogTable)
        } else {
          (false,catalogTable)
        }
      } else {
        (false,catalogTable)
      }
    } else {
      (false,catalogTable)
    }
  }

  def checkBinaryExpression(left: Expression, right: Expression): Boolean = {
    if (checkUnaryExpression(left)) {
      checkUnaryExpression(right)
    } else {
      logError(s"Expression for ${left.getClass} is currently not supported by ClickHouse.")
      false
    }
  }

  def checkSeqExpression(expressions: Seq[Expression]): Boolean = {
    for (expression <- expressions) {
      if (!checkUnaryExpression(expression)) {
        return false
      }
    }
    true
  }

  def checkUnaryExpression(expression: Expression): Boolean = {
    var canConvert: Boolean = false
    expression match {
      // predicate
      case expression: GreaterThanOrEqual =>
        checkBinaryExpression(expression.left, expression.right)
      case expression: GreaterThan =>
        checkBinaryExpression(expression.left, expression.right)
      case expression: LessThanOrEqual =>
        checkBinaryExpression(expression.left, expression.right)
      case expression: LessThan =>
        checkBinaryExpression(expression.left, expression.right)
      case expression: Not =>
        checkUnaryExpression(expression.child)
      case expression: In =>
        checkUnaryExpression(expression.value)
      case expression: InSet =>
        checkUnaryExpression(expression.child)
      case expression: Or =>
        checkBinaryExpression(expression.left, expression.right)
      case expression: And =>
        checkBinaryExpression(expression.left, expression.right)
      case expression: EqualTo =>
        checkBinaryExpression(expression.left, expression.right)

      // nullExpressions
      case expression: IsNotNull =>
        checkUnaryExpression(expression.child)
      case expression: IsNull =>
        checkUnaryExpression(expression.child)

      // aggregate
      case expression: AggregateExpression =>
        checkSeqExpression(expression.children)
      case expression: Sum =>
        checkUnaryExpression(expression.child)
      case expression: Average =>
        checkUnaryExpression(expression.child)
      case expression: Count =>
        checkSeqExpression(expression.children)
      case expression: Max =>
        checkUnaryExpression(expression.child)
      case expression: Min =>
        checkUnaryExpression(expression.child)

      // datetimeExpression
      case expression: Minute =>
        checkUnaryExpression(expression.child)
      case expression: Hour =>
        checkUnaryExpression(expression.child)
      case expression: Second =>
        checkUnaryExpression(expression.child)
      case expression: Year =>
        checkUnaryExpression(expression.child)
      case expression: Month =>
        checkUnaryExpression(expression.child)
      case expression: TruncTimestamp =>
        checkBinaryExpression(expression.left, expression.right)

      // string
      case expression: Contains =>
        checkBinaryExpression(expression.left, expression.right)
      case expression: Length =>
        checkUnaryExpression(expression.child)

      // RegExp
      case expression: RegExpReplace =>
        if (checkUnaryExpression(expression.pos)) {
          if (checkUnaryExpression(expression.rep)) {
            if (checkUnaryExpression(expression.regexp)) {
              if (checkUnaryExpression(expression.subject)) {
                return true
              }
            }
          }
        }
        false

      // arithmetic
      case expression: Subtract =>
        checkBinaryExpression(expression.left, expression.right)
      case expression: Add =>
        checkBinaryExpression(expression.left, expression.right)
      case expression: Multiply =>
        checkBinaryExpression(expression.left, expression.right)
      case expression: Divide =>
        checkBinaryExpression(expression.left, expression.right)

      // conditional
      case expression: CaseWhen =>
        checkSeqExpression(expression.children)

      case expression: SortOrder =>
        checkUnaryExpression(expression.child)
      case expression: Alias =>
        checkUnaryExpression(expression.child)
      case expression: Cast =>
        checkUnaryExpression(expression.child)
      case expression: AttributeReference =>
        true
      case expression: Literal =>
        true
      case expression =>
        logError(s"Expression for ${expression.getClass} is currently not supported by ClickHouse.")
        false
    }
  }

  def replaceWithTransformerPlan(plan: LogicalPlan, sqlPlan: AstTransmitExec): Seq[SparkPlan] = {
    if (sqlPlan.outputSchema.isEmpty) {
      //      val output: Seq[Attribute] = plan.output
      //      output.foreach { attribute =>
      //        println(s"cdx : Column: ${attribute.name}, Type: ${attribute.dataType.sql}")
      //      }
      sqlPlan.setOutputSchema(plan.output)
    }

    var canConvert: Boolean = true
    plan match {
      case plan: ReturnAnswer =>
        replaceWithTransformerPlan(plan.child, sqlPlan)
      case plan: Project =>
        for (project <- plan.projectList) {
          project match {
            case expression: Expression =>
              if (!checkUnaryExpression(expression)) {
                sqlPlan.setCanConvert(false)
                return Nil
              }
            case _ =>
              sqlPlan.setCanConvert(false)
              return Nil
          }
        }
        replaceWithTransformerPlan(plan.child, sqlPlan)
      case plan: Filter =>
        canConvert = {
          plan.condition match {
            case condition: Expression =>
              checkUnaryExpression(condition)
            case _ =>
              false
          }
        }
        if (canConvert) {
          replaceWithTransformerPlan(plan.child, sqlPlan)
        } else {
          sqlPlan.setCanConvert(false)
          Nil
        }
      case plan: LogicalRelation =>
        val checkResult : (Boolean,CatalogTable) = checkTableSchema(plan.catalogTable.get.identifier)
        if (checkResult._1) {
          sqlPlan.setCatalogTable(checkResult._2)
        } else {
          logError(s"Transformation for ${plan.getClass} is currently not supported by ClickHouse.")
          sqlPlan.setCanConvert(false)
        }
        Nil
      case plan: HiveTableRelation =>
        val checkResult : (Boolean,CatalogTable) = checkTableSchema(plan.tableMeta.identifier)
        if (checkResult._1) {
          sqlPlan.setCatalogTable(checkResult._2)
        } else {
          logError(s"Transformation for ${plan.getClass} is currently not supported by ClickHouse.")
          sqlPlan.setCanConvert(false)
        }
        Nil
      case plan: Sort =>
        for (sortOrder <- plan.order) {
          sortOrder match {
            case expression: Expression =>
              if (!checkUnaryExpression(expression)) {
                sqlPlan.setCanConvert(false)
                return Nil
              }
            case _ =>
              sqlPlan.setCanConvert(false)
              return Nil
          }
        }
        replaceWithTransformerPlan(plan.child, sqlPlan)
      case plan: Aggregate =>
        for (grouping <- plan.groupingExpressions) {
          grouping match {
            case expression: Expression =>
              if (!checkUnaryExpression(expression)) {
                sqlPlan.setCanConvert(false)
                return Nil
              }
            case _ =>
              sqlPlan.setCanConvert(false)
              return Nil
          }
        }
        for (aggregate <- plan.aggregateExpressions) {
          aggregate match {
            case expression: Expression =>
              if (!checkUnaryExpression(expression)) {
                sqlPlan.setCanConvert(false)
                return Nil
              }
            case _ =>
              sqlPlan.setCanConvert(false)
              return Nil
          }
        }
        replaceWithTransformerPlan(plan.child, sqlPlan)
      case plan: GlobalLimit =>
        if (checkUnaryExpression(plan.limitExpr)) {
          replaceWithTransformerPlan(plan.child, sqlPlan)
        } else {
          Nil
        }
      case plan: LocalLimit =>
        if (checkUnaryExpression(plan.limitExpr)) {
          replaceWithTransformerPlan(plan.child, sqlPlan)
        } else {
          Nil
        }

      case plan: LogicalQueryStage =>
        Nil

      case plan: Distinct =>
        logError(s"Transformation for ${plan.getClass} is currently not supported by ClickHouse.")
        sqlPlan.setCanConvert(false)
        Nil
      case plan: Sample =>
        logError(s"Transformation for ${plan.getClass} is currently not supported by ClickHouse.")
        sqlPlan.setCanConvert(false)
        Nil
      case plan: Tail =>
        logError(s"Transformation for ${plan.getClass} is currently not supported by ClickHouse.")
        sqlPlan.setCanConvert(false)
        Nil
      case plan: Expand =>
        logError(s"Transformation for ${plan.getClass} is currently not supported by ClickHouse.")
        sqlPlan.setCanConvert(false)
        Nil
      case plan: Range =>
        logError(s"Transformation for ${plan.getClass} is currently not supported by ClickHouse.")
        sqlPlan.setCanConvert(false)
        Nil
      case plan: CreateDataSourceTableCommand =>
        logError(s"Transformation for ${plan.getClass} is currently not supported by ClickHouse.")
        sqlPlan.setCanConvert(false)
        Nil
      case plan: Subquery =>
        logError(s"Transformation for ${plan.getClass} is currently not supported by ClickHouse.")
        sqlPlan.setCanConvert(false)
        Nil
      case plan: Generate =>
        logError(s"Transformation for ${plan.getClass} is currently not supported by ClickHouse.")
        sqlPlan.setCanConvert(false)
        Nil
      case plan: Intersect =>
        logError(s"Transformation for ${plan.getClass} is currently not supported by ClickHouse.")
        sqlPlan.setCanConvert(false)
        Nil
      case plan: Union =>
        logError(s"Transformation for ${plan.getClass} is currently not supported by ClickHouse.")
        sqlPlan.setCanConvert(false)
        Nil
      case plan: Join =>
        logError(s"Transformation for ${plan.getClass} is currently not supported by ClickHouse.")
        sqlPlan.setCanConvert(false)
        Nil
      case plan: InsertIntoDir =>
        logError(s"Transformation for ${plan.getClass} is currently not supported by ClickHouse.")
        sqlPlan.setCanConvert(false)
        Nil
      case plan: View =>
        logError(s"Transformation for ${plan.getClass} is currently not supported by ClickHouse.")
        sqlPlan.setCanConvert(false)
        Nil
      case plan: UnresolvedWith =>
        logError(s"Transformation for ${plan.getClass} is currently not supported by ClickHouse.")
        // Exists in the analysis stage
        sqlPlan.setCanConvert(false)
        Nil
      case plan: CTERelationDef =>
        logError(s"Transformation for ${plan.getClass} is currently not supported by ClickHouse.")
        sqlPlan.setCanConvert(false)
        Nil
      case plan: CTERelationRef =>
        logError(s"Transformation for ${plan.getClass} is currently not supported by ClickHouse.")
        sqlPlan.setCanConvert(false)
        Nil
      case plan: WithCTE =>
        logError(s"Transformation for ${plan.getClass} is currently not supported by ClickHouse.")
        sqlPlan.setCanConvert(false)
        Nil
      case plan: WithWindowDefinition =>
        logError(s"Transformation for ${plan.getClass} is currently not supported by ClickHouse.")
        sqlPlan.setCanConvert(false)
        Nil
      case plan: Window =>
        logError(s"Transformation for ${plan.getClass} is currently not supported by ClickHouse.")
        sqlPlan.setCanConvert(false)
        Nil
      case plan: Pivot =>
        logError(s"Transformation for ${plan.getClass} is currently not supported by ClickHouse.")
        // Exists in the analysis stage
        sqlPlan.setCanConvert(false)
        Nil
      case plan: Repartition =>
        logError(s"Transformation for ${plan.getClass} is currently not supported by ClickHouse.")
        sqlPlan.setCanConvert(false)
        Nil
      case plan: RepartitionByExpression =>
        logError(s"Transformation for ${plan.getClass} is currently not supported by ClickHouse.")
        sqlPlan.setCanConvert(false)
        Nil
      case plan: RebalancePartitions =>
        logError(s"Transformation for ${plan.getClass} is currently not supported by ClickHouse.")
        sqlPlan.setCanConvert(false)
        Nil
      case plan: OneRowRelation =>
        logError(s"Transformation for ${plan.getClass} is currently not supported by ClickHouse.")
        sqlPlan.setCanConvert(false)
        Nil
      case plan: Deduplicate =>
        logError(s"Transformation for ${plan.getClass} is currently not supported by ClickHouse.")
        sqlPlan.setCanConvert(false)
        Nil
      case plan: CollectMetrics =>
        logError(s"Transformation for ${plan.getClass} is currently not supported by ClickHouse.")
        sqlPlan.setCanConvert(false)
        Nil
      case plan: DomainJoin =>
        logError(s"Transformation for ${plan.getClass} is currently not supported by ClickHouse.")
        sqlPlan.setCanConvert(false)
        Nil
      case plan: LateralJoin =>
        logError(s"Transformation for ${plan.getClass} is currently not supported by ClickHouse.")
        sqlPlan.setCanConvert(false)
        Nil
      case plan: AsOfJoin =>
        logError(s"Transformation for ${plan.getClass} is currently not supported by ClickHouse.")
        sqlPlan.setCanConvert(false)
        Nil
      case p =>
        logError(s"Transformation for ${p.getClass} is currently not supported by ClickHouse.")
        sqlPlan.setCanConvert(false)
        Nil

    }
  }


  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    val sqlLeafPlan: AstTransmitExec = AstTransmitExec(plan.conf.getConfString("lindorm.backend.sql"), null, Seq.empty[Attribute])
    val sparkPlans: Seq[SparkPlan] = replaceWithTransformerPlan(plan, sqlLeafPlan)
    if (sqlLeafPlan.canConvert) {
      val seq: Seq[SparkPlan] = Seq(sqlLeafPlan)
      seq
    } else {
      sparkPlans
    }
  }
}

class AstTransmitExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(exts: SparkSessionExtensions): Unit = {
    exts.injectPlannerStrategy(AstTransmitOverrides)
  }
}


