package com.waitingforcode.sparksummit2019.customstatestore

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.util.zip.GZIPInputStream

import com.google.common.io.ByteStreams
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.types.StructType

object UnsafeRowConverter {

  def getStateFromCompressedState(compressedState: Array[Byte], keySchema: StructType,
                                  valueSchema: StructType): (UnsafeRow, UnsafeRow) = {
    val byteArrayInputStream = new ByteArrayInputStream(compressedState)
    val gzipInputStream = new GZIPInputStream(byteArrayInputStream)
    val dataInputStream = new DataInputStream(gzipInputStream)

    // Copy from org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider#updateFromDeltaFile
    val keySize = dataInputStream.readInt()
    val keyRowBuffer = new Array[Byte](keySize)
    ByteStreams.readFully(dataInputStream, keyRowBuffer, 0, keySize)
    val keyRow = new UnsafeRow(keySchema.fields.length)
    val valueSize = dataInputStream.readInt()
    keyRow.pointTo(keyRowBuffer, keySize)
    if (valueSize > -1) {
      val valueRowBuffer = new Array[Byte](valueSize)
      ByteStreams.readFully(dataInputStream, valueRowBuffer, 0, valueSize)
      val valueRow = new UnsafeRow(valueSchema.fields.length)
      valueRow.pointTo(valueRowBuffer, (valueSize / 8) * 8)
      (keyRow, valueRow)
    } else {
      (keyRow, null)
    }
  }

  def compressKeyAndValue(key: UnsafeRow, value: Option[UnsafeRow], isDelete: Boolean): Array[Byte] = {
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val gzipCodec = new GzipCodec().createOutputStream(byteArrayOutputStream)
    val dataOutputStream = new DataOutputStream(gzipCodec)

    val keyBytes = key.getBytes()
    dataOutputStream.writeInt(keyBytes.size)
    dataOutputStream.write(keyBytes)
    if (isDelete) {
      dataOutputStream.writeInt(-1)
    } else {
      val valueBytes = value.get.getBytes()
      dataOutputStream.writeInt(valueBytes.size)
      dataOutputStream.write(valueBytes)
    }
    dataOutputStream.close()
    byteArrayOutputStream.toByteArray
  }

}


