package org.apache.gluten.execution

import org.apache.gluten.substrait.rel.LocalFilesNode
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.RDDScanExec


class RDDScanExecTransformer(output: Seq[Attribute],
     rdd: RDD[InternalRow],
     name: String)
  extends RDDScanExec(output, rdd, name)
    with BasicScanExecTransformer {

  private def rddName: String = Option(rdd.name).map(n => s" $n").getOrElse("")

  override val nodeName: String = s"Scan $name$rddName"

  override def simpleString(maxFields: Int): String = {
    s"$nodeName${truncatedString(output, "[", ",", "]", maxFields)}"
  }

  /** This can be used to report FileFormat for a file based scan operator. */
  override val fileFormat: LocalFilesNode.ReadFileFormat = _

  override def getPartitions: Seq[InputPartition] = partitions



}
