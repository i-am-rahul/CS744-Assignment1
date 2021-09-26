/**
 * Created on 09/23/2021 by Rahul Choudhary
 *
 * PageRank implementation using Apache Spark in Scala.
 * This code makes extensive use of Spark DataFrames and Datasets
 * and their corresponding transformation and actions.
 */

package org.rakab

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.collect_list

object SparkPageRank {

  case class Links(FromNodeId: String, ToNodeId: String)

  def main(args: Array[String]) = {
    var iterations = 20

    if(args.length >= 1) {
      iterations = args(0).toInt
    }

    val spark = SparkSession
      .builder()
      .appName("Spark Page Rank")
      .config("spark.master", "local")
      .getOrCreate()

    val config = ConfigFactory.load("PageRank.conf")

    import spark.implicits._
    val ds = spark.read.text(config.getString("pageRank.inputFile")).as[String]

    // Dataset that maintains each url and its list of outgoing links
    val linksList = ds
      .map[(String, String)]((row: String) => {
        val edge = row.split("\\s+")
        (edge(0), edge(1))
      })
      .dropDuplicates()
      .groupBy("_1")
      .agg(collect_list("_2") as "links")

    // Dataset to keep track of urls and their corresponding ranks. Initialise the rank of each url to 1
    var ranks = linksList.map[(String, Double)]((row: Row) => (row.getString(0), 1.toDouble))

    //Time the for loop
    val t1 = System.nanoTime()

    for(i <- 1 to iterations) {
      var updatedRanks = linksList
        .join(ranks, "_1")
        .flatMap(row => {
          val sz = row.getSeq(1).length
          row.getSeq[String](1).map(link => (link, row.getDouble(2) / sz))
        })
        .groupBy($"_1")
        .sum("_2")
        .map(row => (row.getString(0), 0.15 + 0.85 * row.getDouble(1)))

      ranks = updatedRanks
    }

    ranks.show()

    val duration = (System.nanoTime() - t1) / 1e9d
    println("Time taken to run " + iterations + " iterations = " + duration + " seconds")
  }
}
