package com.gena.exercises.linkedin

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}


object LinkedInMain {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().appName("linked_hw").master("local[2]").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    import session.implicits._

    val df: DataFrame = session.read.json("data/profiles.json")
    df.show()
    df.printSchema()
    df.columns.foreach(e => println(df.col(e).expr.dataType.typeName))

    println("================ Added Salary =====================")
    val withSalary = df.withColumn("salary", col("age") * size(col("keywords")) * 10)
    withSalary.show()

    val mostPopular = df
      .flatMap(e => e.getAs[Seq[String]]("keywords"))
      .groupBy("value").count()
      .orderBy(desc("count"))
      .first().getAs[String]("value")

    val topTech = "\"" + mostPopular + "\""
    println("================ top tech: " + topTech + " =====================")

    //option 1
    withSalary.filter(s"salary <= 1200 and array_contains(keywords, $topTech)").show()

    //Option 2
    withSalary
      .filter(_.getAs[Long]("salary") <= 1200)
      .filter(_.getAs[Seq[String]]("keywords").contains(mostPopular))
      .show()


  }

}
