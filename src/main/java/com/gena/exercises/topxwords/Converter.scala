package com.gena.exercises.topxwords

import scala.collection.JavaConverters._

object Converter {

  def convert(source: Map[String, Int]): java.util.Map[String, java.lang.Integer] = {
    source.map(a => (a._1, a._2.asInstanceOf[java.lang.Integer])).asJava
  }

}
