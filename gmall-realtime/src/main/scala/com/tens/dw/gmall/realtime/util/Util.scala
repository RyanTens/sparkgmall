package com.tens.dw.gmall.realtime.util

import java.io.InputStream
import java.util.Properties

object Util {
  private val is: InputStream = Util.getClass.getClassLoader.getResourceAsStream("config.properties")
  private val properties = new Properties()
  properties.load(is)
  def getProperty(propName: String): String = {
    properties.getProperty(propName)
  }
}
