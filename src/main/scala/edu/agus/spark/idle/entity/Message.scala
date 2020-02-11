package edu.agus.spark.idle.entity

case class Message(Id: String,
                   Name: String,
                   Country: String,
                   City: String,
                   Address: String,
                   Latitude: Double,
                   Longitude: Double,
                   avg_tempr_f: Double,
                   wthr_date: String,
                   geohash: String,
                   precision: Int)
