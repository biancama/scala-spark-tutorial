package com.sparkTutorial.rdd.nasaApacheWebLogs

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SameHostsProblem {

  private def notHeader(entry: String): Boolean = !(entry.startsWith("host") && entry.contains("bytes"))

  private def extractHost(rdd: RDD[String]): RDD[String] =
    rdd.map(line => line.split("\\t")(0))
      .filter(entry => notHeader(entry))
  def main(args: Array[String]) {

    /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
       "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
       Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
       Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

       Example output:
       vagrant.vf.mmc.com
       www-a1.proxy.aol.com
       .....

       Keep in mind, that the original log files contains the following header lines.
       host	logname	time	method	url	response	bytes

       Make sure the head lines are removed in the resulting RDD.
     */
    val conf = new SparkConf().setAppName("Same host").setMaster("local[4]")
    val context = new SparkContext(conf)

    val firstOfJuly = context.textFile("in/nasa_19950701.tsv")
    val firstOfAugust = context.textFile("in/nasa_19950801.tsv")

    extractHost(firstOfJuly)
      .intersection(extractHost(firstOfAugust))
      .saveAsTextFile("out/nasa_logs_same_hosts.csv")
  }
}
