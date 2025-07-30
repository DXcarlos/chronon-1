package ai.chronon.spark

import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions.ListOps
import ai.chronon.api.{DateRange, MetaData}
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.planner.JoinDerivationNode
import ai.chronon.spark.Extensions._
import org.apache.spark.sql.functions.{coalesce, col, expr}

/*
For entities with Derivations (`GroupBy` and `Join`), we produce the pre-derivation `base` table first,
then the derivation job runs as an incremental step.

Note: We always want the "true left" columns on the derivation output, whether they're included in derivation output (*) or not
True left columns are keys, ts, and anything else selected on left source.

Source -> True left table -> Bootstrap table (sourceTable here)
 */
class JoinDerivationJob(node: JoinDerivationNode, metaData: MetaData, range: DateRange)(implicit
    tableUtils: TableUtils) {
  implicit val partitionSpec = tableUtils.partitionSpec
  private val join = node.join
  private val dateRange = range.toPartitionRange
  private val derivations = join.derivations.toScala

  // The true left table is the source table for the join's left side
  private val trueLeftTable = JoinUtils.computeFullLeftSourceTableName(join)

  // The base table is the output of the merge job
  private val baseTable = join.metaData.outputTable

  // Output table for this derivation job comes from the metadata
  private val outputTable = metaData.outputTable

  def run(): Unit = {

    val leftDf = tableUtils.scanDf(query = null, table = trueLeftTable, range = Some(dateRange))
    val trueLeftCols = leftDf.columns

    val baseDf = tableUtils.scanDf(query = null, table = baseTable, range = Some(dateRange))
    val valueCols = baseDf.columns.diff(trueLeftCols)

    val baseOutputColumns = baseDf.columns.toSet

    val projections = derivations.derivationProjection(baseOutputColumns.toSeq)
    val projectionsMap = projections.toMap

    val finalOutputColumns =
      /*
       * Loop through all columns in the base join output:
       * 1. If it is one of the value columns, then skip it here, and it will be handled later as we loop through
       *    derived columns again - derivation is a projection from all value columns to desired derived columns
       * 2.  (see case 2 below) If it is matching one of the projected output columns, then there are 2 subcases
       *     a. matching with a left column, then we handle the "coalesce" here to make sure left columns show on top
       *     b. a bootstrapped derivation case, the skip it here, and it will be handled later as
       *        loop through derivations to perform coalescing
       * 3. Else, we keep it in the final output - cases falling here are either (1) key columns, or (2)
       *    arbitrary columns selected from left.
       */
      baseDf.columns.flatMap { c =>
        if (valueCols.contains(c)) {
          None
        } else if (projectionsMap.contains(c)) {
          if (trueLeftCols.contains(c)) {
            Some(coalesce(col(c), expr(projectionsMap(c))).as(c))
          } else {
            None
          }
        } else {
          Some(col(c))
        }
      } ++
        /*
         * Loop through all clauses in derivation projections:
         * 1. (see case 2 above) If it is matching one of the projected output columns, then there are 2 sub-cases
         *     a. matching with a left column, then we skip since it is handled above
         *     b. a bootstrapped derivation case (see case 2 below), then we do the coalescing to achieve the bootstrap
         *        behavior.
         * 2. Else, we do the standard projection.
         */
        projections
          .flatMap { case (name, expression) =>
            if (baseOutputColumns.contains(name)) {
              if (trueLeftCols.contains(name)) {
                None
              } else {
                Some(coalesce(col(name), expr(expression)).as(name))
              }
            } else {
              Some(expr(expression).as(name))
            }
          }

    baseDf.select(finalOutputColumns: _*).save(outputTable)

  }
}
