package tianz.bd.api.flink.ecommerce

import java.io.File

import org.apache.flink.api.common.state.StateTtlConfig.{StateVisibility, UpdateType}
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @Author: Miaoxf
 * @Date: 2021/1/3 22:25
 * @Description:
 *  实时订单校验,通过connectedStream实现
 *    用户支付完后，需要核对平台账户是否到账。因此需要将两个流的数据合并处理。
 */
object OrderConnectAndCheck {

  //订单数据
  case class OrderEvent(var orderId: String, var orderType: String, var userId: String , var orderTime: Long)
  //账单数据
  case class ReceiptEvent(var userId: String, var payType: String, var payTime: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //TODO 并行度不是1会有什么问题？

    val path = new File("").getCanonicalPath
    val orderSource = env.readTextFile(path + "\\flink\\src\\main\\resources\\order\\orderLog.csv")
    val receiptSource = env.readTextFile(path + "\\flink\\src\\main\\resources\\order\\receiptLog.csv")

    val orderStream: KeyedStream[OrderEvent, String] = orderSource.map(data => {
      val arr = data.split(",")
      OrderEvent(arr(0), arr(1), arr(2), arr(3).toLong)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(3)) {
      override def extractTimestamp(element: OrderEvent): Long = element.orderTime
    }).filter(!_.userId.equals(""))
      .keyBy(_.userId)

    val receiptStream: KeyedStream[ReceiptEvent, String] = receiptSource.map(data => {
      val arr = data.split(",")
      ReceiptEvent(arr(0), arr(1), arr(2).toLong)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ReceiptEvent](Time.seconds(3)) {
      override def extractTimestamp(element: ReceiptEvent): Long = element.payTime
    })keyBy(_.userId)

    val resultStream = orderStream.connect(receiptStream).process(new OrderMatchReceipt)

    val orderTag = new OutputTag[OrderEvent]("receipt timeout")
    val receiptTag = new OutputTag[ReceiptEvent]("order timeout")

    resultStream.print("successfully matched")
    resultStream.getSideOutput(orderTag).print("order timeout")
    resultStream.getSideOutput(receiptTag).print("receipt timeout")

    env.execute()
  }

  //状态变量是存在哪里？jobManager？

  class OrderMatchReceipt extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {

    //TODO 状态变量应该是在jobmanager中，可以共享，便于分布式。而普通的变量只在task中？
    //这边就是keyedState
    var orderState: ValueState[OrderEvent] = null
    var receiptState: ValueState[ReceiptEvent] = null

    //todo 是否需要和上面保持一致？
    val orderTag = new OutputTag[OrderEvent]("receipt timeout")
    val receiptTag = new OutputTag[ReceiptEvent]("order timeout")

    override def open(parameters: Configuration): Unit = {
      val stateTtl = StateTtlConfig
        .newBuilder(org.apache.flink.api.common.time.Time.seconds(30))
        .setUpdateType(UpdateType.OnCreateAndWrite)
        .setStateVisibility(StateVisibility.NeverReturnExpired)
        .build()

      val valueStateDesc = new ValueStateDescriptor[OrderEvent]("orderState", createTypeInformation[OrderEvent])
      valueStateDesc.enableTimeToLive(stateTtl)
      //这边获取的就是KeyedState
      orderState = getRuntimeContext.getState(valueStateDesc)

      val receiptStateDesc = new ValueStateDescriptor[ReceiptEvent]("receiptState", createTypeInformation[ReceiptEvent])
      receiptStateDesc.enableTimeToLive(stateTtl)
      receiptState = getRuntimeContext.getState(receiptStateDesc)
    }

    override def processElement1(value: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      if (receiptState.value() != null) {
        out.collect(value, receiptState.value())
        receiptState.clear()
      } else {
        orderState.update(value)
        ctx.timerService().registerEventTimeTimer(5000)
      }
    }

    override def processElement2(value: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      if (orderState.value() != null) {
        out.collect(orderState.value(), value)
        orderState.clear()
      } else {
        receiptState.update(value)
        ctx.timerService().registerEventTimeTimer(5000)
      }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

      if (orderState.value() != null) {
//        out.collect()
        //TODO 没匹配到，应该输出单个流
        ctx.output(orderTag, orderState.value())
      }
      if (receiptState.value() != null) {
        ctx.output(receiptTag, receiptState.value())
      }

      orderState.clear()
      receiptState.clear()

    }

  }
}
