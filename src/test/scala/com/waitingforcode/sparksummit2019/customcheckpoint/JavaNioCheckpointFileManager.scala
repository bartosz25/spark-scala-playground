package com.waitingforcode.sparksummit2019.customcheckpoint

import java.nio.file
import java.nio.file.{Files, Paths}
import java.util.function.Predicate
import java.util.stream.Collectors

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileStatus, Path, PathFilter}
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.execution.streaming.CheckpointFileManager.CancellableFSDataOutputStream

import scala.collection.mutable

class JavaNioCheckpointFileManager(path: Path, hadoopConf: Configuration) extends CheckpointFileManager {

  private val fs = path.getFileSystem(hadoopConf)

  override def createAtomic(path: Path, overwriteIfPossible: Boolean): CancellableFSDataOutputStream = {
    println(s"#createAtomic(${path}, ${overwriteIfPossible})")
    ActionsTracker.CreateAtomicCalls.append(path.toString)
    val temporaryPath = Paths.get(path.suffix("-tmp").toUri)
    new CancellableFSDataOutputStream(Files.newOutputStream(temporaryPath)) {
      override def cancel(): Unit = {
        Files.delete(temporaryPath)
      }

      override def close(): Unit = {
        super.close()
        Files.move(temporaryPath, Paths.get(path.toUri))
      }
    }
  }

  override def open(path: Path): FSDataInputStream = {
    println(s"#open(${path})")
    // InputStream must be a seekable or position readable - I'm using the same as
    // in the default implementation for the sake of simplicity. Otherwise an exception like this
    // one will occur:
    // java.lang.IllegalArgumentException: In is not an instance of Seekable or PositionedReadable
    fs.open(path)
  }

  override def list(path: Path, filter: PathFilter): Array[FileStatus] = {
    println(s"#list(${path}, ${filter})")
    import scala.collection.JavaConverters._
    Files.list(Paths.get(path.toUri)).filter(new Predicate[file.Path] {
      override def test(localPath: file.Path): Boolean = {
        filter.accept(new Path(localPath.toUri))
      }
    })
      .collect(Collectors.toList()).asScala
      .map(path => {
        val fileStatus = new FileStatus()
        fileStatus.setPath(new Path(path.toUri))
        fileStatus
      })
      .toArray
  }

  override def list(path: Path): Array[FileStatus] = {
    println(s"#list(${path})")
    list(path, new PathFilter { override def accept(path: Path): Boolean = true })
  }

  override def mkdirs(path: Path): Unit = {
    ActionsTracker.MakeDirsCalls.append(path.toString)
    println(s"#mkdirs(${path})")
    Files.createDirectories(Paths.get(path.toUri))
  }

  override def exists(path: Path): Boolean = {
    println(s"#exists(${path})")
    ActionsTracker.ExistencyChecks.append(path.toString)
    Files.exists(Paths.get(path.toUri))
  }

  override def delete(path: Path): Unit = {
    println(s"#delete(${path})")
    if (exists(path)) {
      Files.delete(Paths.get(path.toUri))
    }
  }

  override def isLocal: Boolean = true
}

object ActionsTracker {
  val ExistencyChecks = new mutable.ListBuffer[String]()
  val MakeDirsCalls = new mutable.ListBuffer[String]()
  val CreateAtomicCalls = new mutable.ListBuffer[String]()
}