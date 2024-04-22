package org.apache.gluten.execution

import org.apache.gluten.GlutenConfig
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.rel.RelNode
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch

case class RangeExecTransformer(range: org.apache.spark.sql.catalyst.plans.logical.Range)
  extends UnaryTransformSupport
    with PredicateHelper
    with Logging{

  val start: Long = range.start

  val end: Long = range.end
  val step: Long = range.step
  val numSlices: Int = range.numSlices.getOrElse(session.sessionState.conf.getConf(SQLConf.LEAF_NODE_DEFAULT_PARALLELISM).getOrElse(sparkContext.defaultParallelism))
  val numElements: BigInt = range.numElements
  val isEmptyRange: Boolean = start == end || (start < end ^ 0 < step)

  override def doTransform(context: SubstraitContext): TransformContext =
    super.doTransform(context)

  override protected def getColumnarInputRDDs(plan: SparkPlan): Seq[RDD[ColumnarBatch]] = super.getColumnarInputRDDs(plan)

  override protected lazy val enableNativeValidation: Boolean = _

  override protected def glutenConf: GlutenConfig = super.glutenConf

  override protected def doValidateInternal(): ValidationResult = super.doValidateInternal()

  override protected def doNativeValidation(context: SubstraitContext, node: RelNode): ValidationResult = super.doNativeValidation(context, node)

}
