package com.bonc.record

import com.redislabs.provider.redis.RedisContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object Demo1 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("demo1").setMaster("local");
    conf.set("redis.host", "127.0.0.1") //redis主机ip
    conf.set("redis.port", "6379") //端口号，不填默认为6379
    //conf.set("redis.auth", "hadoop") //用户权限配置
    //conf.set("redis.db","0")  //数据库设置
    conf.set("redis.timeout", "2000") //设置连接超时时间
    val sc = new SparkContext(conf) //SparkContext是spark程序的入口
    val redisContext = new RedisContext(sc)
    val keysRdd = redisContext.fromRedisKeyPattern("user*", 5)

    val schemaStr = "a,b,c"
    val schema = StructType(schemaStr.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    val redisRdd = keysRdd.getKV()
    val tuple = redisRdd.count()
    println(tuple)
    print(redisRdd)
    val hiveContext = new HiveContext(sc) //创建SQLContext
    val rowRdd = redisRdd.map(msg => Row(msg._2.split(",")(0), msg._2.split(",")(1), msg._2.split(",")(2)))
    hiveContext.createDataFrame(rowRdd, schema).registerTempTable("temp")
    val sql = "select * from temp"
    val data = Seq[(String, String)](("high", "111"), ("abc", "222"), ("together", "333"))
    val redisData: RDD[(String, String)] = sc.parallelize(data)

    redisContext.toRedisKV(redisData,1)

    hiveContext.sql(sql).show
  }

}
