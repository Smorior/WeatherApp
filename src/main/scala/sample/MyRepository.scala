package sample

import org.apache.spark.sql.functions.window
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}
import org.apache.spark.sql.functions._

import scala.reflect.runtime.universe._


class MyRepository {

  def sparkRead(sensor: String, spark: SparkSession): DataFrame = {
    spark.read.option("inferSchema", value = true).option("header", value = true).option("delimiter", ";")
      .csv("src/main/resources/WeatherStation_csv_from_20180221_to_20180619/WeatherStation_" + sensor + ".csv",
        "src/main/resources/WeatherStation_csv_from_20180619_to_20190318/WeatherStation_" + sensor + ".csv")
  }

  def calculateMinMaxAvg(durations: Array[String], dataset: Dataset[_], destinationFolder: String, spark: SparkSession) {
    import spark.implicits._
    durations.foreach(duration => {
      val result = dataset.groupBy($"sensor", window($"timestamp", duration)).agg(
        functions.min("value").as("min"),
        functions.max("value").as("max"),
        functions.avg("value").as("avg")
      ).withColumn("start", $"window.start").withColumn("end", $"window.end").orderBy("start").drop("window")

      result.repartition(1).write.mode("overwrite").format("csv").option("header", value = true).option("delimiter", ";").save("weather/" + destinationFolder + "/".concat(duration))
    })
  }

  def calculatePVLTotal(durations: Array[String], dataset: Dataset[_], spark: SparkSession): Unit = {
    import spark.implicits._

    val forHour: Dataset[PluviometerResult] = dataset.groupBy($"sensor", window($"timestamp", "60 minutes", "60 minutes", "2 minutes"))
      .agg(max("value").as("total")).withColumn("start", $"window.start").withColumn("end", $"window.end").orderBy("start").drop("window").as[PluviometerResult]

    forHour.repartition(1).write.mode("overwrite").format("csv").option("header", true).option("delimiter", ";").save("weather/pluviometer/60 minutes")

    if(durations.size > 1) {
      for (i <- 1 until durations.size) {
        val result = forHour.groupBy($"sensor", window($"start", durations(i)))
          .agg(sum("total").as("total")).withColumn("start", $"window.start").withColumn("end", $"window.end").orderBy("start").drop("window").as[PluviometerResult]
        result.repartition(1).write.mode("overwrite").format("csv").option("header", true).option("delimiter", ";").save("weather/pluviometer/"+durations(i))
      }
    }
  }
}
