package com.lcb.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.lcb.GmallConstants
import com.lcb.bean.{CouponAlertInfo, EventLog}
import com.lcb.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks._

object AlterApp2 {
    def main(args: Array[String]): Unit = {
        //创建SparkConf对象
        val spark: SparkConf = new SparkConf().setAppName("AlterApp2").setMaster("local[*]")

        //创建ssc对象
        val ssc = new StreamingContext(spark,Seconds(3))

        //获取kafkaDStream
        val kafkaDStream: InputDStream[(String, String)] = MyKafkaUtil
                .getKafkaStream(ssc,Set(GmallConstants.KAFKA_TOPIC_EVENT))

        val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

        //把数据封装到样例类对象中
        val caseClassDStream: DStream[EventLog] = kafkaDStream.map { case (_, value) => {
            //解析json字符串
            val eventLog: EventLog = JSON.parseObject(value, classOf[EventLog])
            //添加日期和小时字段
            val dateAndHour: Array[String] = sdf.format(new Date(eventLog.ts)).split(" ")
            eventLog.logDate = dateAndHour(0)
            eventLog.logHour = dateAndHour(1)
            //返回样例类对象
            eventLog
        }}
        //预警条件：同一设备，5分钟内三次及以上用不同账号登录并领取优惠劵，并且在登录到领劵过程中没有浏览商品
        //开窗30s
        val windowDStream: DStream[EventLog] = caseClassDStream.window(Seconds(30))
        //按照同一设备id分组
        val groupByMidDStream: DStream[(String, Iterable[EventLog])] = windowDStream
                .map(log => (log.mid, log))
                .groupByKey()

        val awaitFilter: DStream[(Boolean, CouponAlertInfo)] = groupByMidDStream.map { case (mid, logIter) => {
            //创建集合用于存放领取优惠券所对应的uid
            val uids = new util.HashSet[String]()
            //创建集合用于存放领取优惠券所对应的商品id
            val itemIds = new util.HashSet[String]()
            //创建集合用于存放用户行为
            val events = new util.ArrayList[String]()

            //是否点击商品标志
            var noClick: Boolean = true

            breakable {
                logIter.foreach(log => {
                    //添加用户行为
                    events.add(log.evid)
                    //判断是否点击商品行为
                    if ("clickItem".equals(log.evid)) {
                        noClick = false
                        break()
                        //判断是否是领取优惠券行为
                    } else if ("copon".equals(log.evid)) {
                        uids.add(log.uid)
                        itemIds.add(log.itemid)
                    }
                })
            }
            (uids.size() >= 3 && noClick, new CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis))
        }
        }
        val alertLog: DStream[CouponAlertInfo] = awaitFilter.filter(_._1).map(_._2)
        alertLog.print()

        //启动任务
        ssc.start()
        //阻塞main线程
        ssc.awaitTermination()
    }
}
