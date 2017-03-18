package com.sdu.jspark.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author hanhan.zhang
  * */
class JKafkaDataCollector {

  def collector() : Unit = {
    val sparkConfig = new SparkConf(true)

    val sparkContext = new SparkContext(sparkConfig)

    val streamingContext = new StreamingContext(sparkContext, Durations.seconds(1))

    // kafka参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "JK_message_group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topic = Array("JK_Message")

    val locationStrategy = LocationStrategies.PreferBrokers
    val consumerStrategy = ConsumerStrategies.Subscribe(topic, kafkaParams)

    val kafkaStream = KafkaUtils.createDirectStream(streamingContext, locationStrategy, consumerStrategy)

  }

}

object JKafkaDataCollector {

  def apply(): JKafkaDataCollector = new JKafkaDataCollector()

  def main(args: Array[String]): Unit = {
    val kafkaDataCollector = JKafkaDataCollector()
    kafkaDataCollector.collector()
  }

}
