package sample

import java.sql.Timestamp

case class TemperatureResult(sensor : String, start : Timestamp, end: Timestamp, avg: Double, max: Double, min: Double)
