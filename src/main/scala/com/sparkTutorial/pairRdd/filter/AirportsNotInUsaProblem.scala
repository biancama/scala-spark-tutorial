package com.sparkTutorial.pairRdd.filter

import com.sparkTutorial.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object AirportsNotInUsaProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text;
       generate a pair RDD with airport name being the key and country name being the value.
       Then remove all the airports which are located in United States and output the pair RDD to out/airports_not_in_usa_pair_rdd.text

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located,
       IATA/FAA code, ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:

       ("Kamloops", "Canada")
       ("Wewak Intl", "Papua New Guinea")
       ...
     */
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = new SparkConf().setAppName("AirportUsa").setMaster("local[4]")
    val sc = new SparkContext(spark)
    val airportLines = sc.textFile("in/airports.text")
    val airportsNotInUsa = airportLines.map(line => line.split(Utils.COMMA_DELIMITER)).filter(arr => arr(3) != "\"United States\"").map(arr => (arr(1), arr(3)))

    airportsNotInUsa.map { case (name, country) => s"($name, $country)"}.saveAsTextFile("out/airports_not_in_usa_pair_rdd.text")

  }
}
