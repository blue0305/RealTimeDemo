package com.lcb.handler

import com.lcb.bean.StartUpLog
import com.lcb.util.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {

    /**
      * 批次内去重，同一个批次的mid去重
      * @param filterByRedisDStream
      * @return
      */
    def filterDataByRdd(filterByRedisDStream: DStream[StartUpLog]): DStream[StartUpLog] = {

        //变换结果为((data,mid),startUpLog)
        val mapDStream: DStream[((String, String), StartUpLog)] = filterByRedisDStream
                .map(log => ((log.logDate,log.mid),log))
        //按照(log.logDate,log.mid)分组,加上日期是因为同一批次可能会跨天
        val groupDStream: DStream[((String, String), Iterable[StartUpLog])] = mapDStream.groupByKey()
        //按照时间排序,取时间最小的log
        val res: DStream[StartUpLog] = groupDStream.flatMap {
            case ((_, _), iter) => {
                iter.toList.sortWith((x1, x2) => {
                    if (x1.ts < x2.ts) true else false
                }).take(1)
            }
        }
        res
    }


    /**
      * 跨批次去重，当前的mid如果存在当前redis中的set里面，就过滤掉
      *
      * @param startLogDStream
      * @return
      */
    def filterDataByRedis(startLogDStream: DStream[StartUpLog]): DStream[StartUpLog] = {

        startLogDStream.transform(rdd => {

            rdd.mapPartitions(iter => {
                //获取连接
                val jedisClient: Jedis = RedisUtil.getJedisClient
                //过滤掉redis中存在的mid
                val res: Iterator[StartUpLog] = iter.filter(log => {
                    val redisKey = "dau_" + log.logDate
                    !jedisClient.sismember(redisKey, log.mid)
                })
                //关闭连接
                jedisClient.close()

                res
            })
        })
    }


    /**
      * 把mid按天保存到redis中，存入set中，以保证mid不重复
      *
      * @param startLogDStream
      */
    def saveMidToRedis(startLogDStream: DStream[StartUpLog]) = {

        startLogDStream.foreachRDD(startLog => {

            startLog.foreachPartition(iter => {
                //获取jedis连接
                val jedisClient: Jedis = RedisUtil.getJedisClient
                //将数据写入redis,mid用set存储，redis的key为 "dau_日期"
                iter.foreach(log => {
                    val redisKey = "dau_" + log.logDate
                    jedisClient.sadd(redisKey,log.mid)
                })
                //关闭连接
                jedisClient.close()
            })
        })
    }
}
