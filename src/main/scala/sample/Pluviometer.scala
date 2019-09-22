package sample

import java.sql.Timestamp

case class Pluviometer(id: Long, id_wasp: String, id_secret: String, frame_type: Int, frame_number: Int, sensor: String, value: Double, timestamp: Timestamp, sync: Int, raw: String, parser_type: Int, MeshliumID: String)
