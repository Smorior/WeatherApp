package sample

import java.sql.Timestamp

case class HumidityResult(sensor : String, start : Timestamp, end: Timestamp, avg: Double, max: Double, min: Double)
