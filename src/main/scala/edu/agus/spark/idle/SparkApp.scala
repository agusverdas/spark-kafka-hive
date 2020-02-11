package edu.agus.spark.idle

import java.util.{Collections, Properties, UUID}

import edu.agus.spark.idle.deserializer.WeatherHotelDeserializer
import edu.agus.spark.idle.entity.Message
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

object SparkApp {
  def consumeHotelsWeather(): List[Message] = {
    val props= new Properties()
    props.put("group.id", UUID.randomUUID().toString)
    props.put("bootstrap.servers","localhost:6667")
    props.put("key.deserializer", classOf[StringDeserializer])
    props.put("value.deserializer", classOf[WeatherHotelDeserializer])
    props.put("auto.offset.reset", "earliest")

    val consumer = new KafkaConsumer[String, Message](props)
    val topic = "day_weather_hotel"

    val messages: List[Message] = List()

    consumer.subscribe(Collections.singletonList(topic))
    while (consumer) {
      val records = consumer.poll(10).asScala
      for (record <- records.iterator) {
        record.value() :: messages
      }
    }
    messages
  }

 def main(args: Array[String]): Unit = {
   val weatherHotels = consumeHotelsWeather()
   println(weatherHotels)
   val sparkSession = SparkSession.builder().appName("read-avro-from-hdfs").getOrCreate()
   val df = sparkSession.read.format("avro").load("hdfs://localhost:8020/user/hadoop/expedia/part-00000-ef2b800c-0702-462d-b37f-5f2fb3a093d0-c000.avro")
 }

}