package com.atguigu.ordertimeout_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object OrderTimeoutWithStatePro {

  val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")

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

    val orderResultStream: DataStream[OrderResult] = orderEventStream.process(new OrderPayMatch())

    orderResultStream.print("payed")
    orderResultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")

    env.execute("order timeout with state job")
  }

  class OrderPayMatch() extends KeyedProcessFunction[Long, OrderEvent, OrderResult]{

    //定义状态：①用来表示是否支付过②定时器时间戳状态
    lazy val isPayedState : ValueState[Boolean] = getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("isPayed-state", classOf[Boolean])
    )

    lazy val timeState : ValueState[Long] = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("time-state", classOf[Long])
    )

    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
      //先获取当前状态值
      val isPayed: Boolean = isPayedState.value()
      val timeTs: Long = timeState.value()

      //根据事件的type判断，主要是create和pay
      if(value.eventType == "create"){
        //判断是否已经支付过
        if(isPayed){
          //1.如果已经支付过，匹配成功，输出正常支付的信息
          out.collect(OrderResult(value.orderId, "payed successfully"))
          //清空定时器和状态
          ctx.timerService().deleteEventTimeTimer(timeTs)
          isPayedState.clear()
          timeState.clear()
        }else{
          //2.如果没有支付过，注册定时器等待pay事件
          ctx.timerService().registerEventTimeTimer((value.eventTime + 600) * 1000L)
          timeState.update((value.eventTime + 600) * 1000L)
        }
      }else if(value.eventType == "pay"){
        //如果是pay事件，判断是否已经定义了定时器
        if(timeTs > 0){
          //3.有定时器注册，说明已经create，可以输出结果,定时器事件与事件pay的时间做比较
          if(value.eventTime * 1000 < timeTs){
            //如果pay的时间小于定时器触发的时间，说明没有超时，正常输出
            out.collect(OrderResult(value.orderId, "payed successfully"))
          }else{
            //如果大于定时器触发时间，应该报超时
            ctx.output(orderTimeoutOutputTag, OrderResult(value.orderId, "payed but already timeout"))
          }
          //清空定时器和状态
          ctx.timerService().deleteEventTimeTimer(timeTs)
          isPayedState.clear()
          timeState.clear()
        }else{
          //4.如果没有定时器定义，需要等待create事件到来
          isPayedState.update(true)
          //可选，可以注册定时器，等待一段时间，如果create不来，也触发一个报警信息
          ctx.timerService().registerEventTimeTimer(value.eventTime)
          timeState.update(value.eventTime * 1000L)
        }
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, Orde-一种是createrEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
      //两种触发定时器的情形，一种是create注册了定时器等待pay但pay没来，另一种是pay来了create没来
      val isPayed: Boolean = isPayedState.value()
      if(isPayed){
        //1.create没来但pay来了，说明数据丢失或者业务异常
        ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "already payed but create log not found"))
      }else{
        //2.create来了，但是没有pay，真正的超时
        ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "order timeout"))
      }
      isPayedState.clear()
      timeState.clear()
    }
  }
}