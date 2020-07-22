package com.gena.exercises.taxi

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

class DataProcessor extends Serializable {

  private val destination = 1
  private val driverId = 0;
  private val tripLength = 2

  private val driverName = 1;

  def countRows(input: RDD[String]): Long = {
    input.count()
  }

  def filterAndPersistTripsByDestination(trips: RDD[Array[String]], tripDestination: String, storageLevel: StorageLevel): RDD[Array[String]] = {
    val dest = tripDestination.toLowerCase()
    trips.filter(trip => trip(destination).toLowerCase == dest).persist(storageLevel)
  }

  def countTripsByLengthGreaterThan(trips: RDD[Array[String]], minLength: Int): Long = {
    trips
      .filter(trip => trip(tripLength).toInt > minLength)
      .count()
  }

  def countTotalTripsLength(trips: RDD[Array[String]]): Long = {
    trips
      .map(trip => trip(tripLength).toInt)
      .sum().toLong
  }

  def findTopDrivers(trips: RDD[Array[String]], drivers: RDD[Array[String]], top: Int): Array[(String, (Int, String))] = {

    val driversIdsAndNames = drivers.map(driver => (driver(driverId), driver(driverName)))

    trips
      .map(trip => (trip(driverId), trip(tripLength).toInt))
      .reduceByKey(_ + _)
      .join(driversIdsAndNames)
      .sortBy(e => e._2._1, ascending = false) //sort by trip length
      .take(top)
  }

}
