package org.apache.gluten.execution


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.RDDScanExec


class RDDScanExecTransformer(output: Seq[Attribute],
     rdd: RDD[InternalRow],
     name: String)
  extends RDDScanExec(output, rdd, name) {

  private def rddName: String = Option(rdd.name).map(n => s" $n").getOrElse("")

  override val nodeName: String = s"Scan $name$rddName"

  override def simpleString(maxFields: Int): String = {
    s"$nodeName${truncatedString(output, "[", ",", "]", maxFields)}"
  }



}
