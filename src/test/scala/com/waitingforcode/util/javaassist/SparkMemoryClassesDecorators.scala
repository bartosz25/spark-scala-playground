package com.waitingforcode.util.javaassist


import javassist.{ClassPool, CtClass}

object SparkMemoryClassesDecorators {


  private lazy val Classes = ClassPool.getDefault()

  def decorateAcquireStorageMemory(className: String, methodName: String): CtClass = {
    val decoratedClass = Classes.get(className)
    decoratedClass.defrost()
    decoratedClass.getDeclaredMethod(methodName)
      .insertBefore("org.apache.spark.SparkMemoryDecorations.decorateAcquireStorageMemory(blockId, numBytes, memoryMode);")
    decoratedClass.getDeclaredMethod(methodName)
      .insertBefore("org.apache.spark.SparkMemoryDecorations.decorateAcquireUnrollMemory(blockId, numBytes, memoryMode);")
    decoratedClass.writeFile()
    decoratedClass.detach()
    decoratedClass
  }

  def decoratePuts(): CtClass = {
    val decoratedClass = Classes.get("org.apache.spark.storage.BlockManager")
    decoratedClass.defrost()
    val decorations = Seq(
      ("putBlockData", "blockId, data, level, classTag"),
      ("putIterator", "blockId, values, level, tellMaster"),
      ("putBytes", "blockId, bytes, level, tellMaster"),
      ("putSingle", "blockId, value, level, tellMaster"),
      ("doPut", "blockId, level, classTag, tellMaster, keepReadLock")
    )
    decorations.foreach {
      case (methodToDecorate, paramsToGet) => {
        val firstLetter = methodToDecorate.head.toUpper
        val methodDecoration = methodToDecorate.substring(1)
        //println(s"Decorating: ${methodToDecorate} with "+s"org.apache.spark.SparkMemoryDecorations.decorate${firstLetter}${methodDecoration}(${paramsToGet});")
        decoratedClass.getDeclaredMethod(methodToDecorate)
          .insertBefore(s"org.apache.spark.SparkMemoryDecorations.decorate${firstLetter}${methodDecoration}(${paramsToGet});")
      }
    }
    decoratedClass.writeFile()
    decoratedClass.detach()
    decoratedClass
  }


  def decorateTaskMemoryManager(): CtClass = {
    val decoratedClass = Classes.get("org.apache.spark.memory.TaskMemoryManager")
    decoratedClass.defrost()
    decoratedClass.getDeclaredMethod("allocatePage")
      .insertBefore("org.apache.spark.SparkMemoryDecorations.decorateAllocatePage(this, size, consumer);")
    decoratedClass.getDeclaredMethod("cleanUpAllAllocatedMemory")
      .insertBefore("org.apache.spark.SparkMemoryDecorations.decorateCleanUpAllAllocatedMemory(this);")
    decoratedClass.getDeclaredMethod("acquireExecutionMemory")
      .insertBefore("org.apache.spark.SparkMemoryDecorations.decorateAcquireExecutionMemory(required, consumer);")
    decoratedClass.getDeclaredMethod("releaseExecutionMemory")
      .insertBefore("org.apache.spark.SparkMemoryDecorations.decorateReleaseExecutionMemory(size, consumer);")
    decoratedClass.getDeclaredMethod("allocatePage")
      .insertBefore("org.apache.spark.SparkMemoryDecorations.decorateAllocatePage(size, consumer);")
    decoratedClass.writeFile()
    decoratedClass.detach()
    decoratedClass
  }

  def decorateBufferHolder(): CtClass = {
    val decoratedClass = Classes.get("org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder")
    decoratedClass.defrost()
    decoratedClass.getDeclaredMethod("grow").insertBefore(
      "org.apache.spark.SparkMemoryDecorations.decorateGrow(neededSize, row);"
    )
    decoratedClass.writeFile()
    decoratedClass.detach()
    decoratedClass
  }

  def decorateMemoryConsumer(): CtClass = {
    val decoratedClass = Classes.get("org.apache.spark.memory.MemoryConsumer")
    decoratedClass.defrost()
    decoratedClass.getDeclaredMethod("acquireMemory").insertBefore(
      "org.apache.spark.SparkMemoryDecorations.decorateAcquireMemory(size, this);"
    )
    decoratedClass.writeFile()
    decoratedClass.detach()
    decoratedClass
  }

  def decorateMemoryStore(): CtClass = {
    val decoratedClass = Classes.get("org.apache.spark.storage.memory.MemoryStore")
    decoratedClass.defrost()
    decoratedClass.getDeclaredMethod("reserveUnrollMemoryForThisTask").insertBefore(
      "org.apache.spark.SparkMemoryDecorations.decorateReserveUnrollMemoryForThisTask(blockId, memory, memoryMode);"
    )
    decoratedClass.getDeclaredMethod("putBytes").insertBefore(
      "org.apache.spark.SparkMemoryDecorations.decoratePutBytesMemoryStore(blockId, size);")
    decoratedClass.getDeclaredMethod("putIteratorAsValues").insertBefore(
      "org.apache.spark.SparkMemoryDecorations.decoratePuIteratorAsValuesMemoryStore(blockId);")
    decoratedClass.getDeclaredMethod("putIteratorAsBytes").insertBefore(
      "org.apache.spark.SparkMemoryDecorations.decoratePutIteratorAsBytesMemoryStore(blockId);")
    decoratedClass.getDeclaredMethod("remove").insertBefore(
      "org.apache.spark.SparkMemoryDecorations.decorateRemoveMemoryStore(blockId);")
    decoratedClass.getDeclaredMethod("clear").insertBefore(
      "org.apache.spark.SparkMemoryDecorations.decorateClearMemoryStore();")
    decoratedClass.getDeclaredMethod("evictBlocksToFreeSpace").insertBefore(
      "org.apache.spark.SparkMemoryDecorations.decorateEvictBlocksToFreeSpaceMemoryStore(blockId, space);")

    decoratedClass.writeFile()
    decoratedClass.detach()
    decoratedClass
  }

  def decorateUnsafeRow(): CtClass = {
    val decoratedClass = Classes.get("org.apache.spark.sql.catalyst.expressions.UnsafeRow")
    decoratedClass.defrost()
    decoratedClass.getConstructors.foreach(constructor => {
      if (constructor.toString.contains("(I)V")) {
        constructor.insertAfter("org.apache.spark.SparkMemoryDecorations.decorateUnsafeRowConstructor(this);")
      }
    })
    decoratedClass.writeFile()
    decoratedClass.detach()
    decoratedClass
  }

  def decorateHadoopRDD(): CtClass = {
    val decoratedClass = Classes.get("org.apache.spark.rdd.HadoopRDD")
    decoratedClass.defrost()
    decoratedClass.getDeclaredMethod("compute").insertBefore(
      "org.apache.spark.SparkMemoryDecorations.decorateCompute(theSplit);"
    )
    decoratedClass.writeFile()
    decoratedClass.detach()
    decoratedClass
  }

  def decorateExternalAppendOnlyMap(): CtClass = {
    val decoratedClass = Classes.get("org.apache.spark.util.collection.ExternalAppendOnlyMap")
    decoratedClass.defrost()
    decoratedClass.getDeclaredMethod("insert").insertBefore(
      "org.apache.spark.SparkMemoryDecorations.decorateInsertAppendOnlyMap(key);"
    )
    decoratedClass.writeFile()
    decoratedClass.detach()
    decoratedClass
  }
}
