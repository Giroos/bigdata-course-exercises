package com.gena.exercises.taxi

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object MainTaxi {

  def main(args: Array[String]): Unit = {
    val processor = new DataProcessor()
    val sc = new SparkContext(master = "local[2]", appName = "hw1")
    val tripsRdd = sc.textFile("src/main/resources/taxi_orders.txt")
    val driversRdd = sc.textFile("src/main/resources/drivers.txt").map(driver => driver.split(", "))
    val persistedTrips = tripsRdd.map(row => row.split(" ")).persist(StorageLevel.MEMORY_ONLY)

    val tripCount = processor.countRows(tripsRdd)
    val persistedBostonTrips = processor.filterAndPersistTripsByDestination(persistedTrips, "boston", StorageLevel.MEMORY_ONLY)
    val longTripsToBoston = processor.countTripsByLengthGreaterThan(persistedBostonTrips, 10)
    val totalBostonTripsLength = processor.countTotalTripsLength(persistedBostonTrips)
    val topDrivers = processor.findTopDrivers(persistedTrips, driversRdd, 3)

    println(s"Total trips: $tripCount")
    println(s"Trips to Boston longer than 10km: $longTripsToBoston")
    println(s"Total length driven to Boston: $totalBostonTripsLength")
    println("3 drivers with largest total trip distance:")
    topDrivers.foreach(d => println(s"${d._2._2} - ${d._2._1}km"))
  }

}
