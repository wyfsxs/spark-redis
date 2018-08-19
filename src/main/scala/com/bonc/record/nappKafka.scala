

package com.bonc.record

import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import kafka.serializer.StringDecoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import java.text.SimpleDateFormat
import java.util.Date

import com.bonc.util.{MyKafkaUntil, RedisUtil}
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import java.util.Calendar

import com.redislabs.provider.redis.RedisContext
import org.apache.spark.rdd.RDD

import scala.util.Random

//功能：cs信令从kafka沉淀到hdfs分区表中
object nappkafkaTest01 {
  def main(args: Array[String]): Unit = {
    //创建sparkConf
    val conf = new SparkConf().setAppName("nappkafkaTest01").set("spark.streaming.kafka.maxRatePerPartition", "10")
    conf.set("redis.host", "127.0.0.1") //redis主机ip
    conf.set("redis.port", "6379") //端口号，不填默认为6379
    //conf.set("redis.auth", "hadoop") //用户权限配置
    //conf.set("redis.db","0")  //数据库设置
    conf.set("redis.timeout", "2000") //设置连接超时时间
    //创建sparkContext对象，SparkContext是spark程序入口
    val sc = new SparkContext(conf)
    //创建StreamingContext,指定处理批次间隔
    val ssc = new StreamingContext(sc, Seconds(25))
    //创建HiveContext(SqlContext的子类)，使用HiveContext.sql可以执行HQL
    val hiveContext = new HiveContext(sc)
    //创建redisContext，用于操作redis
    val redisContext = new RedisContext(sc)

    //配置Kafka Consumer的相关参数，使用KafkaUtils读取message
    //声明topic列表
    val topics = "HTTP,DNS,STREAMING"
    //val topics="PfDataSedS1MME,nCSPSResult"
    //声明Kafka集群服务器
    val brokers = "172.33.2.6:9092,172.33.2.7:9092,172.33.2.8:9092,172.33.2.9:9092,172.33.2.10:9092"
    //分割各个topic，转为set集合
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    val schemaStr = "MSISDN,STARTTIME,ENDTIME,APP_TYPE,APP_SUB_TYPE,PROCTYPE,RAT,URI,RoamType,ReleaserCode"
    val schema = StructType(schemaStr.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    //获取数据进行加工
    val msgDStream = messages
      //首先获取Kafka消息中的数据(第一个元素是Kafka消息偏移量，第二个元素是数据)
      .map(_._2)
      .filter(msg => msg != null && msg.length > 0)
      //把每行数据拆分成字符串数组
      .map(_.split("\\|", -1)).
      map(m => {
        val data = m.toArray
        println("MSISDN" + data(6))
        //创建对应hive目标表的字段字符串，并生成StructType类型的schema，用于后续创建DataFrame
        if (data.length == 82) {
          val MSISDN = data(6)
          val STARTTIME = data(1)
          val ENDTIME = data(2)
          val APP_TYPE = data(26)
          val APP_SUB_TYPE = data(27)
          val PROCTYPE = data(0)
          val RAT = data(14)
          val URI = data(65)
          (MSISDN, STARTTIME, ENDTIME, APP_TYPE, APP_SUB_TYPE, PROCTYPE, RAT, URI)
        } else {
          val MSISDN = data(6)
          val STARTTIME = data(1)
          val ENDTIME = data(2)
          val APP_TYPE = data(26)
          val APP_SUB_TYPE = data(27)
          val PROCTYPE = data(0)
          val RAT = data(14)
          val URI = ""
          (MSISDN, STARTTIME, ENDTIME, APP_TYPE, APP_SUB_TYPE, PROCTYPE, RAT, URI)
        }
      })

    msgDStream.foreachRDD(rdd => {
      if (!rdd.isEmpty) {
        //将字符串数组rdd映射为RowRDD
        //val rowRdd = rdd.map(msg => Row.fromSeq(msg.slice(0, 10)))
        //将rdd转为RowRdd
        val rowRdd = rdd.map(msg => Row(msg._1, msg._2, msg._3, msg._4, msg._5, msg._6, msg._7, msg._8))
        println(rowRdd.first)
        //使用RowRDD和前边定义好的字段schema创建DataFrame并注册为临时表
        hiveContext.createDataFrame(rowRdd, schema).registerTempTable("nappkafkaTest01")
        println(rowRdd.partitions.length)
        //定义时间格式
        val dateFormat = new SimpleDateFormat("yyyyMMddHH")
        //插入hive表中

        val nowdate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val start_time = nowdate.format(new Date())
        println(start_time + ":一轮文件开始入库！")
        //数据写入HDFS
        val date = dateFormat.format(new Date)
        val p_month = date.substring(0, 6)
        val p_day = date.substring(6, 8)

        val end_time = nowdate.format(new Date())
        println(end_time + ":一轮入库kafka成功!")

        val insertsql = "SELECT MSISDN, FROM_UNIXTIME(UNIX_TIMESTAMP(STARTTIME), 'yyyy-MM-dd HH:mm:ss') STARTTIME, FROM_UNIXTIME(UNIX_TIMESTAMP(ENDTIME), 'yyyy-MM-dd HH:mm:ss') ENDTIME, APP_TYPE, APP_SUB_TYPE, PROCTYPE, RAT,case when URI is null then 'null' else URI end URI FROM nappkafkaTest01 WHERE APP_TYPE = '5'"
        val result = hiveContext.sql(insertsql);
        result.show()

        val producer = MyKafkaUntil.kafkaConn
        //准备调用入文件到redis方法
        val resultList = result.collectAsList()
        println("resultList.size" + resultList.size())
        for (i <- 0 until resultList.size()) {
          println("i的值：" + resultList.get(i))
          val data = resultList.get(i).toString().replaceAll("[\\[\\] ]", "").split(",", -1)
          //print(data)
          val MSISDN = data.apply(0)
          val STARTTIME = data.apply(1)
          val APP_TYPE = data.apply(3)
          val APP_SUB_TYPE = data.apply(4)
          val PROCTYPE = data.apply(5)
          val RAT = data.apply(6)
          val URI = data.apply(7)
          println("MSISDN:" + MSISDN + "STARTTIME:" + STARTTIME + "APP_TYPE:" + APP_TYPE + "APP_SUB_TYPE:" + APP_SUB_TYPE + "PROCTYPE:" + PROCTYPE + "RAT:" + RAT + "URI:" + URI)
          val log_time = STARTTIME.substring(0, 10) + " " + STARTTIME.substring(10, 18)
          println("log_time:" + log_time)
          val now: Date = new Date()
          val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val kafka_time = dateFormat.format(now)
          println(kafka_time)
          val myformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val dnow = myformat.parse(kafka_time)
          val cal = Calendar.getInstance()
          cal.setTime(dnow)
          cal.add(Calendar.SECOND, -Random.nextInt(30))
          val newday = cal.getTime()
          val flume_time = dateFormat.format(newday)
          val interval = (dateFormat.parse(kafka_time).getTime - dateFormat.parse(log_time).getTime) / 1000

          val JedisUtil = RedisUtil.getJedis;

          //var exists = JedisUtil.exists("appTest01" + MSISDN)

          //通过rediscontext查询是否能存在key
          val keysRdd = redisContext.fromRedisKeyPattern("appTest01" + MSISDN, 5)
          //获取redis的key，是否存在
          val cnt = keysRdd.getKV().count()
          val exists = false
          if (cnt == 1) {
            exists == true
          }
          //print("exists的值："+exists)
          if ((MSISDN.length() == 11) && !exists && (interval <= 7200)) {
            val key = MSISDN
            val value = STARTTIME
            //数据写到kafka

            val keymsg = new KeyedMessage[String, String]("appTest01", "{ \"CHANNEL_ID\":\"2\",\"BUSI_CODE\":\"1\",\"LOG_TIME\": \"" + STARTTIME.substring(0, 10) + " " + STARTTIME.substring(10, 18) + "\",\"KAFKA_TIME\": \"" + kafka_time + "\",\"FLUME_TIME\": \"" + flume_time + "\",\"DEVICE_NUMBER\":\"" + MSISDN + "\",\"URL\":\"" + URI + "\",\"APP_TYPE\":\"" + APP_TYPE + "\",\"APP_SUB_TYPE\":\"" + APP_SUB_TYPE + "\",\"RAT\":\"" + RAT + "\"}")

            producer.send(keymsg)
            //JedisUtil.set("appTest01" + key, value)
            //JedisUtil.expire("appTest01" + key, 172800)
            //消息发送到redis
            val data = Seq[(String, String)](("appTest01" + key, value))
            val redisData: RDD[(String, String)] = sc.parallelize(data)
            redisContext.toRedisKV(redisData, 172800)
          }
        }
        producer.close()
      } else println("rdd为空")
    }
    )
    msgDStream.print.toString()
    ssc.start() //启动流计算
    ssc.awaitTermination() //等待直到终止

  }
}






















































