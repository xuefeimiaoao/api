package tianz.bd.api.flink.ecommerce

import java.io.File

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.StateTtlConfig.{StateVisibility, UpdateType}
import org.apache.flink.api.common.state.{StateDescriptor, StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.KeyedStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @Author: Miaoxf
 * @Date: 2021/1/4 15:19
 * @Description:
 *  广告重复点击黑名单，通过state实现
 *    同一用户重复点击广告，也会叠加计算。一段时间内，如果同一用户点击广告的次数超过100，
 *    则会加进黑名单，并不再统计该用户的点击数。
 *
 *    缺点：当黑名单很大时，会导致占用资源很多，影响整个系统的稳定性。
 *
 *    from ali:
 *    https://www.sohu.com/a/391568654_612370
 *    如果你的 operator state 中的 list 长度达到一定规模时，这个 offset 数组就可能会有几十 MB 的规模，
 *    关键这个数组是会返回给 job master，当 operator 的并发数目很大时，很容易触发 job master 的内存超用问题。
 *    我们遇到过用户把 operator state 当做黑名单存储，结果这个黑名单规模很大，导致一旦开始执行 checkpoint，
 *    job master 就会因为收到 task 发来的“巨大”的 offset 数组，而内存不断增长直到超用无法正常响应。
 */
object AdClickBlackListAndCount {

  //广告点击事件
  case class AdClickEvent(var userId: Long, var adId: Long, var province: String, var city: String, var clickTime: Long)

  case class AdCountByProvince(var province: String, var count: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val path = new File("").getCanonicalPath
    val adClickSource = env.readTextFile(path + "\\flink\\src\\main\\resources\\advertisement\\adClickLog.csv")

    //TODO 估算一下一天的活跃用户，如果不大，可以用userId作为Key,这样state也不一定会太大？
    val filteredStream: DataStream[AdClickEvent] = adClickSource.map(data => {
      val arr = data.split(",")
      AdClickEvent(arr(0).toLong, arr(1).toLong, arr(2), arr(3), arr(4).toLong)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[AdClickEvent](Time.seconds(10)) {
      override def extractTimestamp(element: AdClickEvent): Long = element.clickTime
    }).keyBy(_.userId) /*.filterWithState()*/ .process(new ClickCountWithBlackList(10))

    val blackListStream: DataStream[AdClickEvent] = filteredStream.getSideOutput(new OutputTag[AdClickEvent]("clickTime overhead"))

    val resultStream: DataStream[AdCountByProvince] = filteredStream.keyBy(_.province).timeWindow(Time.hours(1), Time.minutes(5)).aggregate(new PreAggr, new WindowFunc)

    resultStream.print("clickCount")
    blackListStream.print("blackList")

    env.execute("AdClickBlackListAndCount")
  }

  class ClickCountWithBlackList(var maxClickTimes: Int) extends KeyedProcessFunction[Long, AdClickEvent, AdClickEvent]{

    //每个userId对应的点击数
    var countState: ValueState[Long] = null
    var inBlackList: ValueState[Boolean] = null

    override def open(parameters: Configuration): Unit = {
      val ttlConfig = StateTtlConfig
        .newBuilder(org.apache.flink.api.common.time.Time.hours(5))
        .setUpdateType(UpdateType.OnCreateAndWrite)
        .setStateVisibility(StateVisibility.NeverReturnExpired)
        .build()

      val clickCountDesc = new ValueStateDescriptor[Long]("clickCount", createTypeInformation[Long])
      clickCountDesc.enableTimeToLive(ttlConfig)
      countState = getRuntimeContext.getState(clickCountDesc)

      val inBlackListDesc = new ValueStateDescriptor[Boolean]("isInBlackList", createTypeInformation[Boolean])
      inBlackListDesc.enableTimeToLive(ttlConfig)
      inBlackList = getRuntimeContext.getState(inBlackListDesc)
    }

    override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[Long, AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {
      //如果在黑名单中，则不参与接下来的计算
      if (inBlackList.value()) {
        return
      }

      if (countState.value() == null) {
        //第一次统计
        countState.update(1)
        //注册定时器，5小时后清理state
        val timer = ctx.timerService().currentProcessingTime() + 5*3600*1000
        ctx.timerService().registerEventTimeTimer(timer)
      } else {
        countState.update(countState.value() + 1)
      }

      //加入黑名单
      if (countState.value() > maxClickTimes) {
        inBlackList.update(true)
        //TODO 注意ctx.output与out.collect的区别
        ctx.output(new OutputTag[AdClickEvent]("clickTime overhead"), value)
        countState.clear()
        return
      }

      //正常数据
      out.collect(value)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
      //TODO 彻底清理状态
      countState.clear()
      inBlackList.clear()
    }
  }

  class PreAggr extends AggregateFunction[AdClickEvent, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: AdClickEvent, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    //不同分区数据聚合
    override def merge(a: Long, b: Long): Long = a + b
  }

  class WindowFunc extends WindowFunction[Long, AdCountByProvince, String, TimeWindow] {
    //input 即为预聚合函数的返回值
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdCountByProvince]): Unit = {
      out.collect(AdCountByProvince(key, input.sum))
    }
  }
}
