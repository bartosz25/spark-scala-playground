package com.waitingforcode.util.javaassist

import javassist.{ClassPool, CtClass}

object MethodInvocationDecorator {

  private lazy val Classes = ClassPool.getDefault()

  def decorateClass(className: String, methodName: String): CtClass = {
    val decoratedClass = Classes.get(className)
    decoratedClass.getDeclaredMethod(methodName)
      .insertBefore("com.waitingforcode.util.javaassist.MethodInvocationCounter.addNewInvocation(\""+methodName+"\");")
    decoratedClass.writeFile()
    decoratedClass.detach()
    decoratedClass
  }

}

