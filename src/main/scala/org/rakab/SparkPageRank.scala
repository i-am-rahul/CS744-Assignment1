/**
 * Created on 09/23/2021 by Rahul Choudhary
 *
 * PageRank implementation using Apache Spark in Scala.
 * This code makes extensive use of Spark DataFrames and Datasets
 * and their corresponding transformation and actions.
 */

package org.rakab

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.{Encoders, Row, SaveMode, SparkSession}

object SparkPageRank {

  case class Links(FromNodeId: String, ToNodeId: String)

  def main(args: Array[String]) = {
    val log = LogManager.getRootLogger()
    log.setLevel(Level.INFO)

    log.info("Building Spark Session")

    val spark = SparkSession
      .builder()
      .appName("Spark Page Rank")
      .config("spark.master", "local")
      .getOrCreate()

    // Take paths from the user as command line arguments
    val inputPath = args(0)
    val outputPath = args(1)

    var iterations = 10

    if(args.length >= 3) {
      iterations = args(2).toInt
    }

    log.info(s"Loaded config inputPath->$inputPath, outputPath->$outputPath")
    log.info(s"Total iterations = $iterations")

    import spark.implicits._
    log.info("Reading data into df")
    // Create schema object to read the data
    val linkSchema = Encoders.product[Links].schema
    // Read csv data into the dataframe
    var df = spark.read.schema(linkSchema).option("header", false).option("delimiter", "\t").csv(inputPath)

    // Filtering out any null values
    df = df
      .filter(row => {
        row != null && row.getString(0) != null && row.getString(1) != null
      }) // Trim any whitespaces before or after the values
      .map(r => (r.getString(0).toLowerCase().trim, r.getString(1).toLowerCase().trim))
      .filter(r => r._2.contains("category:") || !r._2.contains(":"))
      .toDF("_1", "_2")

    log.info("Creating links")

    // Dataset that maintains each url and its list of outgoing links
    val linksList = df
      .dropDuplicates()
      .groupBy("_1")
      .agg(collect_list("_2") as "links")

    log.info("Creating initial ranks")

    // Dataset to keep track of urls and their corresponding ranks. Initialise the rank of each url to 1
    var ranks = linksList.map[(String, Double)]((row: Row) => (row.getString(0), 1.toDouble))

//    linksList.show()
//    ranks.show()

    //Time the for loop
    val t1 = System.nanoTime()

    log.info("Starting iterations")

    for(i <- 1 to iterations) {
      log.info(s"Creating intmRanks ${i}")
      var updatedRanks = linksList
        .join(ranks, "_1")
        .flatMap(row => {
          val sz = row.getSeq(1).length
          row.getSeq[String](1).map(link => (link, row.getDouble(2) / sz)) ++ Seq[(String, Double)]((row.getString(0), 0))
        })
        .groupBy($"_1")
        .sum("_2")
        .map(row => (row.getString(0), 0.15 + 0.85 * row.getDouble(1)))

      ranks = updatedRanks
    }

//    ranks.show()
    log.info(s"Creating final ranks of all left side articles")

    var finalRanks = linksList.join(ranks, Seq("_1"), "left_outer").drop("links")
//    finalRanks.show()

    log.info(s"Writing to output $outputPath")

    finalRanks.write.mode(SaveMode.Overwrite).option("headers", true).csv(outputPath)

    val duration = (System.nanoTime() - t1) / 1e9d
    println("Time taken to run " + iterations + " iterations = " + duration + " seconds")
  }
}
