package com.gena.exercises.linkedin

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}


object LinkedInMain2 {

  def getMultiplier() {}

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().appName("linked_hw").master("local[2]").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")

    val df: DataFrame = session.read.json("data/profiles.json")

    val withSalary = df
      .withColumn("salary", when(col("age") > 30, col("age")).otherwise(5) * size(col("keywords")))
      .persist()
    withSalary.show()

    val mostPopular = withSalary
      .withColumn("technology", explode(col("keywords")))
      .groupBy("technology")
      .count
      .sort(desc("count"))
      .first.get(0)

    println("================ top tech: " + mostPopular + " =====================")

    //changed max salary to 40
    withSalary
      .filter(_.getAs[Long]("salary") <= 40)
      .filter(array_contains(col("keywords"), mostPopular))
      .show()


  }

}
