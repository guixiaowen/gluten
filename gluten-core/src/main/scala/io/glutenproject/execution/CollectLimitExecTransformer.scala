package io.glutenproject.execution

import java.util.concurrent.atomic.AtomicInteger

import io.glutenproject.extension.{GlutenPlan, ValidationResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, SinglePartition}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.{ColumnarCollapseTransformStages, ColumnarShuffleExchangeExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.vectorized.ColumnarBatch

case class CollectLimitExecTransformer (limit: Int, child: SparkPlan, offset: Long)
  extends UnaryExecNode
  with GlutenPlan {

  override def supportsColumnar: Boolean = true

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

  override protected def doValidateInternal(): ValidationResult = {
    LimitTransformer(child, offset, limit).doValidate()
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  override def output: Seq[Attribute] = child.output


  override def simpleString(maxFields: Int): String = {
    s"CollectLimitExecTransformer (limit=$limit)"
  }

  override def outputPartitioning: Partitioning = SinglePartition

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val childRDD = child.executeColumnar()
    val childRDDPartsNum = childRDD.getNumPartitions

    if (childRDDPartsNum == 0) {
      sparkContext.parallelize(Seq.empty, 1)
    } else {
      val transformStageCounter: AtomicInteger =
        ColumnarCollapseTransformStages.transformStageCounter
      val finalLimitPlan = if (childRDD.getNumPartitions == 1) {
        LimitTransformer(child, offset, limit)
      } else {

        val limitBeforeShuffle = child match {
          case wholeStage: WholeStageTransformer =>
            LimitTransformer(wholeStage.child, 0, limit)
          case other =>
            LimitTransformer(ColumnarCollapseTransformStages.wrapInputIteratorTransformer(other), 0, limit)
        }
        val limitStagePlan =
          WholeStageTransformer(limitBeforeShuffle)(transformStageCounter.incrementAndGet())
        val shuffleExec = ShuffleExchangeExec(SinglePartition, limitStagePlan)
        val transformedShuffleExec =
          ColumnarShuffleExchangeExec(shuffleExec, limitStagePlan, shuffleExec.child.output)
        LimitTransformer(transformedShuffleExec, offset, limit)
      }

      val finalPlan =
          WholeStageTransformer(finalLimitPlan)(transformStageCounter.incrementAndGet())
        finalPlan.executeColumnar()

      }
    }
  }
