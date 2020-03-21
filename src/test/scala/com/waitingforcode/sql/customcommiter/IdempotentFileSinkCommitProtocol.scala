package com.waitingforcode.sql.customcommiter

import org.apache.spark.internal.io.HadoopMapReduceCommitProtocol

class IdempotentFileSinkCommitProtocol(jobId: String, path: String,
                                       dynamicPartitionOverwrite: Boolean = false)
  extends HadoopMapReduceCommitProtocol(jobId = EnvVariableSimulation.JOB_ID, path, dynamicPartitionOverwrite) {

}
