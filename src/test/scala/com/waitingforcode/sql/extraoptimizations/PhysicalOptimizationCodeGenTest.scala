package com.waitingforcode.sql.extraoptimizations

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.execution.SparkPlan


case class UnionJoinExecutorExec2(left: SparkPlan, right: SparkPlan) extends SparkPlan { //with CodegenSupport {

  override protected def doExecute(): RDD[InternalRow] = {
    val leftRdd = left.execute().groupBy(row => {
      row.hashCode()
    })
    val rightRdd = right.execute().groupBy(row => {
      row.hashCode()
    })

    leftRdd.fullOuterJoin(rightRdd).mapPartitions( matchedRows => {
      val rowWriter = new UnsafeRowWriter(6)
      matchedRows.map {
        case (_, (leftRows, rightRows)) => {
          rowWriter.reset()
          rowWriter.zeroOutNullBytes()

          val matchedRow = leftRows.getOrElse(rightRows.get).toSeq.head
          val (letter, nr, flag) = if (matchedRow.isNullAt(0)) {
            (matchedRow.getUTF8String(3), matchedRow.getInt(4), matchedRow.getInt(5))
          } else {
            (matchedRow.getUTF8String(0), matchedRow.getInt(1), matchedRow.getInt(2))
          }
          rowWriter.write(0, letter)
          rowWriter.write(1, nr)
          rowWriter.write(2, flag)
          rowWriter.write(3, letter)
          rowWriter.write(4, nr)
          rowWriter.write(5, flag)
          rowWriter.getRow
        }
      }
    })
  }

  override def output: Seq[Attribute] =  left.output  ++ right.output //

  override def children: Seq[SparkPlan] = Seq(left, right)



  /* TODO: codegen may be a second part of the series !
  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    val leftRdd = children(0).execute().groupBy(row => {
      row.hashCode()
    })
    val rightRdd = children(1).execute().groupBy(row => {
      row.hashCode()
    })

    Seq(leftRdd.fullOuterJoin(rightRdd).flatMap {
      case (_, (leftRows, rightRows)) => {
        leftRows.getOrElse(rightRows.get)
      }
    })
  }

  override protected def doProduce(ctx: CodegenContext): String = {
    val input = ctx.addMutableState("scala.collection.Iterator", "input",
      v => s"$v = inputs[0];")
    val row = ctx.freshName("row")
    ctx.INPUT_ROW = row
    ctx.currentVars = null
    // Generates output variables
    val outputVars = output.zipWithIndex.map { case (a, i) =>
      BoundReference(i, a.dataType, a.nullable).genCode(ctx)
    }
    // Always provide `outputVars`, so that the framework can help us build unsafe row if the input
    // row is not unsafe row, i.e. `needsUnsafeRowConversion` is true.
    //val projection = UnsafeProjection.create(output)
    s"""
       |while ($input.hasNext()) {
       |  InternalRow $row = (InternalRow) $input.next();
       |  System.out.println("Got row $row");
       |  ${consume(ctx, outputVars, row).trim}
       |  if (shouldStop()) return;
       |}
     """.stripMargin
    // ${consume(ctx, outputVars, row).trim}
  }*/
}