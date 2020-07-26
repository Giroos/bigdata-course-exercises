package com.gena.exercises.topxwords

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Lazy
import org.springframework.stereotype.Service

@Service
@Lazy(false)
class SparkService(
                    @Value("${spark.app.name}") appName: String,
                    @Value("${spark.master}") sparkMaster: String,
                    @Value("${spark.files.base.path}") basePath: String,
                    @Value("${spark.garbage.words.file}") garbageWordsFile: String
                  ) extends Serializable {


  @transient private val sc = new SparkContext(master = sparkMaster, appName = appName)
  println(s"initialized spark context with master=$sparkMaster , appName=$appName")
  @transient private val garbageWords: Array[String] = sc.textFile(garbageWordsFile).flatMap(_.split(" ")).collect() //map(_.split(" ")).reduce(_ ++ _)
  private val garbageWordsBroadcast: Broadcast[Array[String]] = sc.broadcast(garbageWords)
  print(s"broadcasted array of ${garbageWords.size} : ")
  garbageWords.foreach(w => print(w + " "))
  println()

  def countTopX(fileName: String, top: Int): Map[String, Int] = {
    val path = basePath + fileName
    println(s"starting counting words: file=$path top=$top")

    sc.textFile(path)
      .flatMap(_.split(" ").map(_.toLowerCase))
      .filter(!garbageWordsBroadcast.value.contains(_))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .take(top).toMap
  }

}
