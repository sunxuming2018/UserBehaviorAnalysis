package com.atguigu.hotitems_analysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.BufferedSource


object KafkaProducer {

  def main(args: Array[String]): Unit = {
    writeToKafka("hostitems")
  }

  def writeToKafka(topic: String): Unit ={
    val properties : Properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop113:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](properties)
    val source: BufferedSource = io.Source.fromFile("D:\\MyWork\\HBase\\Codes\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    for(line <- source.getLines()){
      producer.send(new ProducerRecord[String, String](topic, line))
    }

    producer.close()
  }

}
