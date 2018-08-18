package com.bonc.util

/**
 * kafka工具类
 * author by hetao
 */
import kafka.producer.Producer
import java.util.Properties
import kafka.producer.ProducerConfig

//kafka生产者，用于将加工完的数据推送到kafka
object MyKafkaUntil extends Serializable {
  def kafkaConn(): Producer[String, String] = {
    val props = new Properties()
    //配置kafka Producer所需的一些参数
    props.put("metadata.broker.list", "130.84.209.23:9092,130.84.209.26:9092")
    props.put("zookeeper.connect", "130.84.209.23:2181,130.84.209.26:2181")
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    val config: ProducerConfig = new ProducerConfig(props)
    val producer: Producer[String, String] = new Producer[String, String](config)
    producer
  }
}