package com.tens.dw.gmall.canal

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object MyKafkaUtil {
  private val props = new Properties()
  props.put("bootstrap.servers", "hadoop101:9092,hadoop102:9092,hadoop103:9092")
  // key的序列化
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  // value序列化
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  private val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

  def send(topic: String, content: String): Unit ={
    producer.send(new ProducerRecord[String, String](topic, content))

  }
}
