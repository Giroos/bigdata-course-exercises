package com.gena.exercises.taxi

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.StructType

class RddToDataFrame {

  def convert(rdd: RDD[String], schema: StructType)(implicit spark: SparkSession): DataFrame = {
    null
  }

}
