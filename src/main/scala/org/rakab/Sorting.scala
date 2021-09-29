package org.rakab

import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}

object Sorting {

  case class Parameters(batteryLevel: Int,
                        c02Level: Int,
                        shortCountryCode: String,
                        longCountryCode: String,
                        countryName: String,
                        deviceId: Int,
                        deviceName: String,
                        humidity: Int,
                        ip: String,
                        latitude: Double,
                        lcd: String,
                        longitude: Double,
                        tempScale: String,
                        temp: Double,
                        timestamp: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("RunSpark Application")
      .getOrCreate()

    // Take paths from the user as command line arguments
    val inputPath = args(0)
    val outputPath = args(1)

    // Create schema object to read the data
    val parametersSchema = Encoders.product[Parameters].schema

    // Read csv data into the dataframe
    val df = spark.read.schema(parametersSchema).option("header", true).csv(inputPath)
    df.persist // Cache data to MEMORY and DISK

    df.createOrReplaceTempView("df") // Create a view to be used in spark.sql

    // Country code (3rd column) named as shortCountryCode and (last) column named as timestamp in the schema
    val sortedDf = spark.sql("SELECT * FROM df ORDER BY shortCountryCode, timestamp")

    // Repartitoned with numPartitions = 1 to create a single output csv file
    // Overwrite mode overwrites any older output which might have been present
    sortedDf.repartition(1).write.format("csv").mode(SaveMode.Overwrite).save(outputPath)

  }

}
