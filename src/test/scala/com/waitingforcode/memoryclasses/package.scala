package com.waitingforcode

import java.io.File

import org.apache.commons.io.FileUtils

package object memoryclasses {

  private[memoryclasses] val BasePath = "/tmp/memory_classes"

  private def fileName(fileIndex: Int): String = s"${BasePath}/file_${fileIndex}.json"

  private[memoryclasses] def createTestFilesIfMissing(linesPerFile: Int, files: Int): Unit = {
    for (fileIndex <- 1 to files) {
      val fileToWrite = new File(fileName(fileIndex))
      if (!fileToWrite.exists) {
        val testDataset = (1 to linesPerFile).map(nr => s"""{"id": ${nr}, "labels": {"long":"Number ${nr}/${fileIndex}", "short": "#${nr}/${fileIndex}"}, "creation_time_ms": ${System.currentTimeMillis()}}""")
          .mkString("\n")
        FileUtils.writeStringToFile(fileToWrite, testDataset)
      }
    }
  }

}

case class NumberContent(id: Int, label: NumberContentLabels, creationTimeMs: Long)
case class NumberContentLabels(long: String, short: String)