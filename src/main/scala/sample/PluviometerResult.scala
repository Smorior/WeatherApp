package sample

import java.sql.Timestamp

case class PluviometerResult(sensor : String, total : Double, start : Timestamp, end : Timestamp)
