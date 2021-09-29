/**
 * Created on 09/23/2021 by Rahul Choudhary
 *
 * PageRank implementation using Apache Spark in Scala.
 * This code makes extensive use of Spark DataFrames and Datasets
 * and their corresponding transformation and actions.
 */

package org.rakab

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.functions.{col, collect_list}
import org.apache.spark.sql.{Encoders, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

object SparkPageRank {

  case class Links(FromNodeId: String, ToNodeId: String)

  def main(args: Array[String]) = {
    // Setup logging
    val log = LogManager.getRootLogger()
    log.setLevel(Level.INFO)

    log.info("Building Spark Session")

    // Create SparkSession object
    val spark = SparkSession
      .builder()
      .appName("Spark Page Rank")
      .config("spark.local.dir", "/mnt/data/tmp/")
      .getOrCreate()

    // Take paths from the user as command line arguments
    val inputPath = args(0)
    val outputPath = args(1)

    val persistData = "true".equals(args(2).toLowerCase)  // Check whether to persist or not
    val partitionByCol: String = args(3)                  // Check which column of linksList dataframe to partition on
    val partitionByNum: Int = args(4).toInt               // Check how many partitions to have

    var iterations = 10   // Number of iterations of the PageRank algorithm

    log.info(s"Loaded config inputPath->$inputPath, outputPath->$outputPath, persistData->$persistData, " +
      s"partitionByCol->$partitionByCol, partitionByNum->$partitionByNum")

    log.info(s"Total iterations = $iterations")

    import spark.implicits._
    log.info("Reading data into df")

    // Create schema object to read the data
    val linkSchema = Encoders.product[Links].schema

    // Read csv data into the dataframe
    var df = spark.read.schema(linkSchema).option("header", false).option("delimiter", "\t").csv(inputPath)

    // Filter out any null values in the data, or fromNodes containing ':' except fromNodes containing 'Category:' at the start
    df = df
      .filter(row => {
        row != null && row.getString(0) != null && row.getString(1) != null
      }) // Trim any whitespaces before or after the values
      .map(r => (r.getString(0).toLowerCase().trim, r.getString(1).toLowerCase().trim))
      .filter(r => r._2.contains("category:") || !r._2.contains(":"))
      .toDF("fromNode", "toNode")

    log.info("Creating links")

    // Dataset that maintains each url and its list of outgoing links
    val linksList = df
      .dropDuplicates()
      .groupBy("fromNode")
      .agg(collect_list("toNode") as "links")
      .toDF("node", "links")

    // Persist linksList dataframe to main memory if persistData is passed true by user
    if(persistData)
      linksList.persist(StorageLevel.MEMORY_ONLY)   // Cache data to MEMORY

    // Create partitioning either on 'node' (fromNode) column or partition entire dataframe with user supplied number of partitions
    if(partitionByNum == -1)
      linksList.repartition(col(partitionByCol))
    else
      linksList.repartition(partitionByNum)

    log.info("Creating initial ranks")

    // Dataset to keep track of urls and their corresponding ranks. Initialise the rank of each url to 1
    var ranks = linksList
      .map[(String, Double)]((row: Row) => (row.getString(0), 1.toDouble))
      .toDF("node", "rank")

    // Create partitions on column 'node'
    ranks.repartition(col("node"))

    //Time the for loop (iterations of the PageRank algorithm)
    val t1 = System.nanoTime()

    log.info("Starting iterations")

    for(i <- 1 to iterations) {
      log.info(s"Creating intermediate ranks ${i}")
      // Update ranks after calculating the sum of contributions for each node
      var updatedRanks = linksList
        .join(ranks, "node")
        .flatMap(row => {
          val sz = row.getSeq(1).length
          row.getSeq[String](1).map(link => (link, row.getDouble(2) / sz)) ++ Seq[(String, Double)]((row.getString(0), 0))    // Add (node, 0) to account for nodes with zero incoming links
        })
        .groupBy($"_1")
        .sum("_2")
        .map(row => (row.getString(0), 0.15 + 0.85 * row.getDouble(1)))
        .toDF("node", "rank")

      ranks = updatedRanks                          // Update ranks dataframe
      ranks.repartition(col("node"))      // Create partitions on column 'node'
    }

    log.info(s"Creating final ranks of all left side nodes/articles")

    // For final ranks, only consider the nodes on the left side in the linksList dataframe (i.e. nodes with one or more outgoing edges)
    var finalRanks = linksList.join(ranks, Seq("node"), "left_outer").drop("links")

    log.info(s"Writing to output $outputPath")

    // Write finalRanks as csv data into the output directory
    finalRanks.write.mode(SaveMode.Overwrite).option("headers", true).csv(outputPath)

    val duration = (System.nanoTime() - t1) / 1e9d
    println("Time taken to run " + iterations + " iterations = " + duration + " seconds")
  }
}
