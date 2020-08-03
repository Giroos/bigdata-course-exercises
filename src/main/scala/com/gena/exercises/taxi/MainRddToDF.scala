package com.gena.exercises.taxi

import java.sql.Date
import java.time.{LocalDate}
import java.time.format.DateTimeFormatter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object MainRddToDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("rddtoDf").getOrCreate()
    val rdd: RDD[String] = spark.sparkContext.textFile("src/main/resources/taxi_orders.txt")
    val rowRdd = rdd.map(_.split(" "))
      .map(e => Row(
        e(0).toLong,
        e(1),
        e(2).toLong,
        Date.valueOf(LocalDate.parse(e(3), DateTimeFormatter.ofPattern("dd/MM/yy")))
      ))

    rowRdd.foreach(println(_))


    val schema = StructType.apply(List(
      StructField("driver_id", LongType, false),
      StructField("destination", StringType, false),
      StructField("length", LongType, false),
      StructField("date", DateType, false)
    ))

    val dataFrame = spark.createDataFrame(rowRdd, schema)
    dataFrame.show()
  }
}
