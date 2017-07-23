package com.waitingforcode.util.javaassist

import scala.collection.mutable

object MethodInvocationCounter {

    val methodInvocations = mutable.HashMap[String, Int]()

    def addNewInvocation(methodName: String) = {
        val currentCalls = methodInvocations.getOrElse(methodName, 0)
        methodInvocations(methodName) = currentCalls + 1
    }

}
