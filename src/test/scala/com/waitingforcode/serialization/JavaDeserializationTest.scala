package com.waitingforcode.serialization

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable

class JavaDeserializationTest extends FlatSpec with Matchers {

  "JVM singleton" should "be serialized and deserialized as the same object instance" in {
    val seenHashCodes = mutable.Set[Int]()
    val serializedObjectBytes = serializeObject(SingletonClass)

    for (i <- 1 to 10) {
      val objectInputStream = new ObjectInputStream(new ByteArrayInputStream(serializedObjectBytes))
      val deserializedSingleton = objectInputStream.readObject().asInstanceOf[SingletonClass.type]
      seenHashCodes += deserializedSingleton.hashCode()
      objectInputStream.close()
    }

    seenHashCodes.size shouldEqual(1)
  }

  "Java object" should "be serialized and deserialized as new instance" in {
    val seenHashCodes = mutable.Set[Int]()
    val instanceClass = new InstanceClass
    val serializedObjectBytes = serializeObject(instanceClass)

    for (i <- 1 to 10) {
      val objectInputStream = new ObjectInputStream(new ByteArrayInputStream(serializedObjectBytes))
      val deserializedInstanceClass = objectInputStream.readObject().asInstanceOf[InstanceClass]
      seenHashCodes.add(deserializedInstanceClass.hashCode())
      objectInputStream.close()
    }

    seenHashCodes.size should be > 1
  }

  "Java object with implemented equality" should "be serialized and deserialized as the same intance" in {
    val seenHashCodes = mutable.Set[Int]()
    val instanceClass = new InstanceClassWithEquality(1)
    val serializedObjectBytes = serializeObject(instanceClass)

    for (i <- 1 to 10) {
      val objectInputStream = new ObjectInputStream(new ByteArrayInputStream(serializedObjectBytes))
      val deserializedInstanceClass = objectInputStream.readObject().asInstanceOf[InstanceClassWithEquality]
      seenHashCodes.add(deserializedInstanceClass.hashCode())
      objectInputStream.close()
    }

    seenHashCodes.size shouldEqual(1)
  }

  private def serializeObject(toSerialize: Any): Array[Byte] = {
    val outputStream = new ByteArrayOutputStream()
    val objectOutputStream = new ObjectOutputStream(outputStream)
    objectOutputStream.writeObject(toSerialize)
    objectOutputStream.close()
    outputStream.toByteArray
  }

}

class InstanceClass extends Serializable {}

object SingletonClass extends Serializable {}

class InstanceClassWithEquality(val id: Int) extends Serializable {

  override def equals(comparedObject: scala.Any): Boolean = {
    if (comparedObject.isInstanceOf[InstanceClassWithEquality]) {
      val comparedInstance = comparedObject.asInstanceOf[InstanceClassWithEquality]
      id == comparedInstance.id
    } else {
      false
    }
  }

  override def hashCode(): Int = {
    id
  }

}