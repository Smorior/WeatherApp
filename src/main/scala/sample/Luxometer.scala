package sample

import java.sql.Timestamp

case class Luxometer(id: Long, id_wasp: String, id_secret: String, frame_type: Int, frame_number: Int, sensor: String, value: Int, timestamp: Timestamp, sync: Int, raw: String, parser_type: Int, MeshliumID: String)
