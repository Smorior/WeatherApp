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

//    val temperatureDF : Dataset[Temperature] = repo.sparkRead("TC", spark).as[Temperature].cache()
//
//    val humidityDF : Dataset[Humidity] = repo.sparkRead("HUM", spark).as[Humidity].cache()
//
//    val luxometerDF : Dataset[Luxometer] = repo.sparkRead("LUX", spark).as[Luxometer].cache()
//
//    val anemometerDF : Dataset[Anemometer] = repo.sparkRead("ANE", spark).as[Anemometer].cache()

    val pluviometerDF : Dataset[Pluviometer] = repo.sparkRead("PLV1", spark).as[Pluviometer].cache()


//    val temperatureDF : Dataset[Temperature] = spark.read.option("inferSchema", value = true).option("header", value = true).option("delimiter",";")
//      .csv("src/main/resources/WeatherStation_csv_from_20180221_to_20180619/WeatherStation_TC.csv","src/main/resources/WeatherStation_csv_from_20180619_to_20190318/WeatherStation_TC.csv").as[Temperature].cache()
//
//    val humidityDF : Dataset[Humidity] = spark.read.option("inferSchema", value = true).option("header", value = true).option("delimiter",";")
//        .csv("src/main/resources/WeatherStation_csv_from_20180221_to_20180619/WeatherStation_HUM.csv", "src/main/resources/WeatherStation_csv_from_20180619_to_20190318/WeatherStation_HUM.csv").as[Humidity].cache()
//
//    val luxometerDF : Dataset[Luxometer] = spark.read.option("inferSchema", value = true).option("header", value = true).option("delimiter",";")
//      .csv("src/main/resources/WeatherStation_csv_from_20180221_to_20180619/WeatherStation_LUX.csv", "src/main/resources/WeatherStation_csv_from_20180619_to_20190318/WeatherStation_LUX.csv").as[Luxometer].cache()
//
//    val anemometerDF : Dataset[Anemometer] = spark.read.option("inferSchema", value = true).option("header", value = true).option("delimiter",";")
//      .csv("src/main/resources/WeatherStation_csv_from_20180221_to_20180619/WeatherStation_ANE.csv", "src/main/resources/WeatherStation_csv_from_20180619_to_20190318/WeatherStation_ANE.csv").as[Anemometer].cache()

//    val pluviometerDF = spark.read.option("inferSchema", value = true).option("header", value = true).option("delimiter",";")
//      .csv("src/main/resources/WeatherStation_csv_from_20180221_to_20180619/WeatherStation_PLV1.csv", "src/main/resources/WeatherStation_csv_from_20180619_to_20190318/WeatherStation_PLV1.csv").cache()

//    repo.calculateMinMaxAvg(Array[String](Durations.oneHour, Durations.oneDay, Durations.month), temperatureDF, "temperature", spark)
//    repo.calculateMinMaxAvg(Array[String](Durations.oneHour, Durations.oneDay, Durations.month), humidityDF, "humidity", spark)
//    repo.calculateMinMaxAvg(Array[String](Durations.oneHour, Durations.oneDay, Durations.month), luxometerDF, "luxometer", spark)
//    repo.calculateMinMaxAvg(Array[String](Durations.oneHour, Durations.oneDay, Durations.month), anemometerDF, "anemometer", spark)

      repo.calculatePVLTotal(Array(Durations.oneHour, Durations.oneDay, Durations.month), pluviometerDF, spark)


//    val plvRes : Dataset[AnemometerResult] = pluviometerDF.groupBy($"sensor", window($"timestamp","60 minutes", "60 minutes", "2 minutes"))
//      .agg(max("value").as("total")).withColumn("start", $"window.start").withColumn("end", $"window.end").orderBy("start").drop("window").as[AnemometerResult]
//
//    plvRes.repartition(1).write.mode("overwrite").format("csv").option("header", true).option("delimiter",";").save("test/plv")





    //    durations.foreach(duration => {
//      val temperatureResults : Dataset[TemperatureResult] = temperatureDF.groupBy($"sensor", window($"timestamp", duration)).agg(
//        functions.min("value").as("min"),
//        functions.max("value").as("max"),
//        functions.avg("value").as("avg")
//      ).withColumn("start", $"window.start").withColumn("end", $"window.end").orderBy("start").drop("window").as[TemperatureResult]
//
//      temperatureResults.repartition(1).write.mode("overwrite").format("csv").option("header", value = true).option("delimiter",";").save("weather/temperature/".concat(duration))
//    })
//
//    durations.foreach(duration => {
//      val humidityResults : Dataset[HumidityResult] = humidityDF.groupBy($"sensor", window($"timestamp", duration)).agg(
//        functions.min("value").as("min"),
//        functions.max("value").as("max"),
//        functions.avg("value").as("avg")
//      ).withColumn("start", $"window.start").withColumn("end", $"window.end").orderBy("start").drop("window").as[HumidityResult]
//
//      humidityResults.repartition(1).write.mode("overwrite").format("csv").option("header", true).option("delimiter",";").save("weather/humidity/".concat(duration))
//    })
//
//    durations.foreach(duration => {
//      val luxometerResults : Dataset[LuxometerResult] = luxometerDF.groupBy($"sensor", window($"timestamp", duration)).agg(
//        functions.min("value").as("min"),
//        functions.max("value").as("max"),
//        functions.avg("value").as("avg")
//      ).withColumn("start", $"window.start").withColumn("end", $"window.end").orderBy("start").drop("window").as[LuxometerResult]
//
//      luxometerResults.repartition(1).write.mode("overwrite").format("csv").option("header", true).option("delimiter",";").save("weather/luxometer/".concat(duration))
//    })
//
//    durations.foreach(duration => {
//      val anemometerResults : Dataset[AnemometerResult] = anemometerDF.groupBy($"sensor", window($"timestamp", duration)).agg(
//        functions.min("value").as("min"),
//        functions.max("value").as("max"),
//        functions.avg("value").as("avg")
//      ).withColumn("start", $"window.start").withColumn("end", $"window.end").orderBy("start").drop("window").as[AnemometerResult]
//
//      anemometerResults.repartition(1).write.mode("overwrite").format("csv").option("header", true).option("delimiter",";").save("weather/anemometer/".concat(duration))
//    })

//    durations.foreach(duration => {
//    })
//    val ts = $"timestamp".cast("timestamp").cast("long")
//    val interval = (round(ts / 3000L) * 3000.0).cast("timestamp").alias("interval")
//    pluviometerDF.groupBy($"sensor", interval).sum("value").orderBy("interval").show()
//    val w = Window.partitionBy($"sensor").orderBy("ts").rangeBetween(-150, 150)
//   // pluviometerDF.withColumn("ts", ts).wi
//   // pluviometerDF.show()
//    val lagCol = lag(col("value"), 1).over(w)
//    pluviometerDF.withColumn("ts", ts).withColumn("LagCol", lagCol).show()

//    val w = Window.orderBy("timestamp")
////    val plvRes = pluviometerDF.groupBy($"sensor", $"value", window($"timestamp","60 minutes")).agg(
////      functions.sum("value")
////    ).withColumn("start", $"window.start").withColumn("end", $"window.end").orderBy("start").drop("window")
//
//    val plvRes = pluviometerDF.groupBy($"sensor", $"value", window($"timestamp","60 minutes")).count()
//      .withColumn("start", $"window.start").withColumn("end", $"window.end").orderBy("start")
//      .drop("window").drop("count").groupBy($"start", $"end").sum("value")


    //    temperatureDF.printSchema()
//
//    val temperatureResultsByHour : Dataset[TemperatureResult] = temperatureDF.groupBy($"sensor", window($"timestamp","60 minutes")).agg(
//      functions.min("value").as("min"),
//      functions.max("value").as("max"),
//      functions.avg("value").as("avg")
//    ).withColumn("start", $"window.start").withColumn("end", $"window.end").orderBy("start").drop("window").as[TemperatureResult]
//
//    temperatureResultsByHour.repartition(1).write.format("csv").option("header", true).save("weather/tempByHour")

//    val temperatureResultsByDay : Dataset[TemperatureResult] = temperatureDF.groupBy($"sensor", window($"timestamp","1 day")).agg(
//      functions.min("value").as("min"),
//      functions.max("value").as("max"),
//      functions.avg("value").as("avg")
//    ).withColumn("start", $"window.start").withColumn("end", $"window.end").orderBy("start").drop("window").as[TemperatureResult]
//
//    temperatureResultsByDay.repartition(1).write.format("csv").option("header", true).save("weather/tempByDay")


    System.in.read

//      map(row => TemperatureResult(row.getAs("sensor"), row.getAs("start").toString(),
//      row.getAs("end").toString(), row.getAs[Double]("avg").formatted("%06.2f").toString(),
//      row.getAs("max").toString(), row.getAs("min")).toString()).show()

    //avgValues.write.format("csv").save("weather/temp.csv")
    //avgValues.show()

  }

  def sliceTimestampToHour(timestamp: java.sql.Timestamp): String = {
    val tempTimestamp : String = timestamp.toString().slice(0,13)
    return tempTimestamp
  }

  def timeStampToLong(timestamp: java.sql.Timestamp): Long ={
    return timestamp.getTime()
  }


}
