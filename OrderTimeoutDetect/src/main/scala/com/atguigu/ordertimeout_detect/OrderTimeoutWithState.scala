package com.atguigu.ordertimeout_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object OrderTimeoutWithState {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //1.读取输入的订单数据流
    val orderEventStream = env.fromCollection(List(
      OrderEvent(1, "create", 1558430842),
      OrderEvent(2, "create", 1558430843),
      OrderEvent(2, "other", 1558430845),
      OrderEvent(2, "pay", 1558430850),
      OrderEvent(1, "pay", 1558431920)
    )).assignAscendingTimestamps(_.eventTime * 1000)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(8)) {
        override def extractTimestamp(element: OrderEvent): Long = element.eventTime * 1000
      })
      .keyBy(_.orderId)

    val timeoutWarningStream: DataStream[OrderResult] = orderEventStream.process(new OrderTimeoutWarning)

    timeoutWarningStream.print()

    env.execute("order timeout job")
  }

}

class OrderTimeoutWarning() extends KeyedProcessFunction[Long, OrderEvent, OrderResult]{

  //声明一个状态，用来表示是否支付过
  lazy val isPayedState : ValueState[Boolean] = getRuntimeContext.getState(
    new ValueStateDescriptor[Boolean]("isPayed-state", classOf[Boolean])
  )

  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
    //先获取当前是否支付的状态
    val isPayed = isPayedState.value()
    if (value.eventType == "create" && !isPayed) {
      //如果没有支付过，遇到create事件，注册定时器等待pay事件
      ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 600 * 1000L)
    }else if(value.eventType == "pay"){
      isPayedState.update(true)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    //根据是否支付的状态来判断是否输出报警
    val isPayed = isPayedState.value()
    if(!isPayed){
      out.collect(OrderResult(ctx.getCurrentKey, "order timeout"))
    }else{
      out.collect(OrderResult(ctx.getCurrentKey, "order payed successfully"))
    }
    isPayedState.clear()
  }
}