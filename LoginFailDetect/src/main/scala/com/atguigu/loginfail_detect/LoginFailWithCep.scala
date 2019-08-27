package com.atguigu.loginfail_detect

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object LoginFailWithCep {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val loginEventStream: DataStream[LoginEvent] = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    )).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(1)) {
      override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000
    })

    //定义模式匹配
    val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("begin")
      .where(_.eventType == "fail")
      .next("next")
      .where(_.eventType == "fail")
      .within(Time.seconds(2))

    //在数据流中匹配出定义好的模式
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream.keyBy(_.userId), loginFailPattern)

    //select方法传入一个pattern select function，当检测到定义好的模式序列时就会调用
    val loginFailDataStream: DataStream[Warning] = patternStream.select(new PatternSelectFunction[LoginEvent, Warning] {
      override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
        val first: LoginEvent = map.get("begin").iterator().next()
        val second: LoginEvent = map.get("next").iterator().next()
        Warning(first.userId, first.eventTime, second.eventTime, "在2秒内连续登录失败")
      }
    })

    //将匹配到的符合条件的事件打印出来
    loginFailDataStream.print("result")
    env.execute("Login Fail Detect Job")
  }

}