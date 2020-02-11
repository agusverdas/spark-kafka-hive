package edu.agus.spark.idle.deserializer;

import java.util

import com.google.gson.Gson
import edu.agus.spark.idle.entity.Message
import org.apache.kafka.common.serialization.Deserializer
import java.nio.charset.{Charset, StandardCharsets}

class WeatherHotelDeserializer extends Deserializer[Message] {
  val charset: Charset = StandardCharsets.UTF_8;
  val gson = new Gson()

  override def deserialize(topic: String, data: Array[Byte]): Message = {
    val info = new String(data, charset)
    gson.fromJson(info, classOf[Message])
  }

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}
}
