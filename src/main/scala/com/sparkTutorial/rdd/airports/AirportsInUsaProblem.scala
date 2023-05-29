package com.sparkTutorial.rdd.airports

import com.sparkTutorial.commons.Utils
import org.apache.spark.{SparkConf, SparkContext}

object AirportsInUsaProblem {
  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
       and output the airport's name and the city's name to out/airports_in_usa.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "Putnam County Airport", "Greencastle"
       "Dowagiac Municipal Airport", "Dowagiac"
       ...
     */
    val conf = new SparkConf().setAppName("airports").setMaster("local[2]")
    val sparkContext = new SparkContext(conf)

    val airportLines = sparkContext.textFile("in/airports.text")
    val usaAirports = airportLines.map(_.split(Utils.COMMA_DELIMITER)).filter(arr => arr(3) == "\"United States\"").map(arr => (arr(1), arr(3)))

    usaAirports.map {case (city, country) => s"$city, $country"}.saveAsTextFile("out/airports_in_usa.text")
  }
}
