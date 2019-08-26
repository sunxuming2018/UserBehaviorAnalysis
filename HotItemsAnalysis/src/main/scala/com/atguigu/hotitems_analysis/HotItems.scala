package com.atguigu.hotitems_analysis

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object HotItems {
  //统计近1小时内的热门商品，每5分钟更新一次
  //热门度用浏览次数（“pv”）来衡量
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置时间特性为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //val stream: DataStream[String] = env.readTextFile("")
    val properties : Properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop113:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("hostitems", new SimpleStringSchema(), properties))

    stream.map { record =>
      val arr: Array[String] = record.split(",")
      UserBehavior(arr(0).trim.toLong, arr(1).trim.toLong, arr(2).trim.toInt, arr(3).trim, arr(4).trim.toLong)
    }.filter(_.hehavior == "pv")
      .assignAscendingTimestamps(_.timestamp * 1000)
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg(), new WindowResult())
      .keyBy(_.windowEnd)
      .process(new TopNHotItems(3))
      .print("hostitems")

    env.execute("hot items")
  }

}

//自定义预聚合函数，来一个数据就加一
class CountAgg() extends  AggregateFunction[UserBehavior, Long, Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

//自定义窗口函数，包装成ItemViewCount输出
class WindowResult() extends WindowFunction[Long,ItemViewCount,Long,TimeWindow]{
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val windowEnd: Long = window.getEnd
    val count: Long = input.iterator.next()
    out.collect(ItemViewCount(key, windowEnd, count))
  }
}

//自定义process function,排序处理结果
class TopNHotItems(nSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String]{

  //定义一个list state，用来保存所有的ItemViewCount
  private var itemState : ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemstate", classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    //每一条数据都存入state中
    itemState.add(value)
    //注册定时器，延迟触发；当定时器触发时，当前windowEnd的一组数据应该都到齐，统一排序处理
    context.timerService().registerEventTimeTimer(value.windowEnd + 100)
  }


  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //定时器触发时，已经收集到所有数据,首先把所有数据放到一个list中
    val allItems: ListBuffer[ItemViewCount] = new ListBuffer[ItemViewCount]()
    import scala.collection.JavaConversions._
    for(item <- itemState.get()){
      allItems += item
    }
    itemState.clear()

    //按照count大小排序
    val sortedItems: ListBuffer[ItemViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(nSize)

    //将数据排名信息格式化成String,方便打印输出
    val result: StringBuilder = new StringBuilder()
    result.append("=============================================\n")
    result.append("时间：").append(new Timestamp(timestamp - 100)).append("\n")

    //每一个商品信息输出
    for(i <- sortedItems.indices){
      val currentItem = sortedItems(i)
      result.append("No").append(i + 1).append(": ")
        .append("商品ID=").append(currentItem.itemId)
        .append(" 浏览量=").append(currentItem.count)
        .append("\n")
    }
    result.append("=============================================\n")
    //控制输出频率
    Thread.sleep(1000)
    out.collect(result.toString())
  }

}

//输入数据的样例类
case class UserBehavior(
                       userId : Long,
                       itemId : Long,
                       categoryId : Int,
                       hehavior : String,
                       timestamp : Long
                       )

//中间输出的商品浏览量的样例类
case class ItemViewCount(
                        itemId : Long,
                        windowEnd : Long,
                        count : Long
                        )