package com.lcb.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.lcb.GmallConstants
import com.lcb.bean.StartUpLog
import com.lcb.handler.DauHandler
import com.lcb.util.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
object DauApp {

    def main(args: Array[String]): Unit = {

        //1.创建SparkConf
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TestKafka")

        //2.创建StreamingContext
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        //3.读取Kafka数据创建DStream
        val kafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set
        (GmallConstants.KAFKA_TOPIC_START))

        //定义时间处理格式
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

        //4.将json转换为样例类对象
        val startLogDStream: DStream[StartUpLog] = kafkaDStream.map { case (_, json) =>

            //将json转换为样例类对象
            val log: StartUpLog = JSON.parseObject(json, classOf[StartUpLog])
            //获取时间戳
            val ts: Long = log.ts
            //用sdf把时间戳格式化成字符串
            val dateAndHour: String = sdf.format(new Date(ts))
            //按照空格切分，因为sdf定义的格式是空格切分
            val dateAndHourArr: Array[String] = dateAndHour.split(" ")
            //为logDate和logHour属性赋值
            log.logDate = dateAndHourArr(0)
            log.logHour = dateAndHourArr(1)
            //返回样例类对象
            log
        }
        //跨批次去重
        val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterDataByRedis(startLogDStream)
        //同批次去重
        val filterByBatch: DStream[StartUpLog] = DauHandler.filterDataByRdd(filterByRedisDStream)
        //把数据去重后的mid保存到redis
        DauHandler.saveMidToRedis(filterByBatch)
        //把有效数据数据写入HBase中
        import org.apache.phoenix.spark._
        filterByBatch.foreachRDD{rdd=>
            rdd.saveToPhoenix("GMALL_DAU",Seq("MID", "UID", "APPID", "AREA", "OS", "CH",
                "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS") ,
                new Configuration,Some("hadoop105,hadoop106,hadoop107:2181"))
        }
        filterByBatch.count().print()
        //5.启动任务
        ssc.start()
        ssc.awaitTermination()

    }
}
