package sample

import java.sql.Timestamp

case class LuxometerResult(sensor : String, start : Timestamp, end: Timestamp, avg: Double, max: Int, min: Int)
