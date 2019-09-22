package sample

import java.sql.Timestamp

case class AnemometerResult(sensor : String, start : Timestamp, end: Timestamp, total: Double)
