package sample

import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.{Dataset, SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object WeatherApp {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("Weather Station").master("local[*]").getOrCreate()
    import spark.implicits._

    val repo = new MyRepository()

    val temperatureDF : Dataset[Temperature] = repo.sparkRead("TC", spark).as[Temperature].cache()
    val humidityDF : Dataset[Humidity] = repo.sparkRead("HUM", spark).as[Humidity].cache()
    val luxometerDF : Dataset[Luxometer] = repo.sparkRead("LUX", spark).as[Luxometer].cache()
    val anemometerDF : Dataset[Anemometer] = repo.sparkRead("ANE", spark).as[Anemometer].cache()
    val pluviometerDF : Dataset[Pluviometer] = repo.sparkRead("PLV1", spark).as[Pluviometer].cache()


    repo.calculateMinMaxAvg(Array[String](Durations.oneHour, Durations.oneDay, Durations.month), temperatureDF, "temperature", spark)
    repo.calculateMinMaxAvg(Array[String](Durations.oneHour, Durations.oneDay, Durations.month), humidityDF, "humidity", spark)
    repo.calculateMinMaxAvg(Array[String](Durations.oneHour, Durations.oneDay, Durations.month), luxometerDF, "luxometer", spark)
    repo.calculateMinMaxAvg(Array[String](Durations.oneHour, Durations.oneDay, Durations.month), anemometerDF, "anemometer", spark)
    repo.calculatePVLTotal(Array(Durations.oneHour, Durations.oneDay, Durations.month), pluviometerDF, spark)


    System.in.read


  }

}
