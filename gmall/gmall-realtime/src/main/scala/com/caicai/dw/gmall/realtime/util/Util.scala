package com.caicai.dw.gmall.realtime.util

import java.io.InputStream
import java.util.Properties

/**
  * Author caicai
  * Date 2020/2/11 14:06
  */
object Util {
    private val is: InputStream = Util.getClass.getClassLoader.getResourceAsStream("config.properties")
    private val properties = new Properties()
    properties.load(is)
    def getProperty(propName: String): String = {
        properties.getProperty(propName)
    }
}
