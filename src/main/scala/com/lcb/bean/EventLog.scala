package com.lcb.bean

case class EventLog(mid:String,
                     uid:String,
                     appid:String,
                     area:String,
                     os:String,
                     ch:String,
                     `type`:String,
                     evid:String,
                     pgid:String,
                     npgid:String,
                     itemid:String,
                     var logDate:String,
                     var logHour:String,
                     var ts:Long)
