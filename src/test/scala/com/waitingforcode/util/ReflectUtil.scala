package com.waitingforcode.util

import java.lang.reflect.Field

object ReflectUtil {

  def getDeclaredField[T](fieldClass: Class[T], fieldName: String): Field = {
    val searchedField = fieldClass.getDeclaredField(fieldName)
    searchedField.setAccessible(true)
    searchedField
  }

}
