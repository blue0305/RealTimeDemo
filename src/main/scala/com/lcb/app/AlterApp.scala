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
object AlterApp {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlterApp")

        val ssc = new StreamingContext(sparkConf,Seconds(3))

        //读取kafka数据，创建DStream
        val kafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc,
            Set(GmallConstants.KAFKA_TOPIC_EVENT))

        //定义时间格式
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

        //将每一条元素转换为样例类对象
        val EventLogDStream: DStream[EventLog] = kafkaDStream.map {
            case (_, value) => {
                //解析json字符串，转变为样例类对象
                val log: EventLog = JSON.parseObject(value, classOf[EventLog])
                //补充日期与小时字段
                val ts: Long = log.ts
                val dateAndHour: Array[String] = sdf.format(new Date(ts)).split(" ")
                log.logDate = dateAndHour(0)
                log.logHour = dateAndHour(1)
                //返回样例类对象
                log
            }
        }
        //EventLogDStream.print()
        //预警条件：同一设备，5分钟内三次及以上用不同账号登录并领取优惠劵，并且在登录到领劵过程中没有浏览商品
        //开窗30秒
        val windowDStream: DStream[EventLog] = EventLogDStream.window(Seconds(30))
        //转换结构，按设备id分组
        val groupByMidDStream: DStream[(String, Iterable[EventLog])] = windowDStream
                .map(log => (log.mid, log))
                .groupByKey()

        //是否使用用户id,是否点击了商品
        val awaitFilterDStream: DStream[(Boolean, CouponAlertInfo)] = groupByMidDStream.map { case (mid, logIter) => {

            //创建集合用于存放领取优惠券所对应的uid
            val uids = new util.HashSet[String]()
            //创建集合用于存放领取优惠券所对应的商品id
            val itemIds = new util.HashSet[String]()
            //创建集合用于存放用户行为
            val events = new util.ArrayList[String]()

            //是否有浏览商品行为的标志位
            var noClick: Boolean = true;
            breakable{
                //遍历log迭代器
                logIter.foreach(log => {
                    //添加用户行为
                    events.add(log.evid)
                    //判断是否存在浏览商品行为
                    if ("clickItem".equals(log.evid)) {
                        noClick = false
                        //存在浏览商品的行为就直接跳出，因为不可能产生预警日志
                        break()
                    //判断是否是领取优惠券行为
                    } else if ("copon".equals(log.evid)) {
                        //添加uid和商品id
                        uids.add(log.uid)
                        itemIds.add(log.itemid)
                    }
                })
            }
            //构建成一个元组，前面是boolean值，判断是否是预警日志，后面是存储的预警日志
            (uids.size() >= 3 && noClick, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis))
        }}
        //过滤，留下真正的预警日志
        val earlyWarnDStream: DStream[CouponAlertInfo] = awaitFilterDStream.filter(_._1).map(_._2)

        earlyWarnDStream.print()

        //启动任务
        ssc.start()
        //阻塞main线程
        ssc.awaitTermination()
    }
}
