package tianz.bd.api.flink.ecommerce

import java.io.File

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import tianz.bd.api.flink.ecommerce.OrderConnectAndCheck.{OrderEvent, ReceiptEvent}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @Author: Miaoxf
 * @Date: 2021/1/4 11:09
 * @Description:
 *  实时订单校验,通过intervalJoin实现
 *    用户支付完后，需要核对平台账户是否到账。
 *    缺点：不会输出未匹配到的数据。
 */
object OrderJoinAndCheck {

  //订单数据
  case class OrderEvent(var orderId: String, var orderType: String, var userId: String , var orderTime: Long)
  //账单数据
  case class ReceiptEvent(var userId: String, var payType: String, var payTime: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val path = new File("").getCanonicalPath
    val orderSource = env.readTextFile(path + "\\flink\\src\\main\\resources\\order\\orderLog.csv")
    val receiptSource = env.readTextFile(path + "\\flink\\src\\main\\resources\\order\\receiptLog.csv")

    val orderStream = orderSource.map(data => {
      val arr = data.split(",")
      OrderEvent(arr(0), arr(1), arr(2), arr(3).toLong)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(3)) {
      override def extractTimestamp(element: OrderEvent): Long = element.orderTime
    }).filter(!_.userId.equals(""))
      .keyBy(_.userId)

    val receiptStream = receiptSource.map(data => {
      val arr = data.split(",")
      ReceiptEvent(arr(0), arr(1), arr(2).toLong)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ReceiptEvent](Time.seconds(3)) {
      override def extractTimestamp(element: ReceiptEvent): Long = element.payTime
    }).keyBy(_.userId)

    val result: DataStream[(OrderEvent, ReceiptEvent)] = orderStream.intervalJoin(receiptStream).between(Time.minutes(-1), Time.minutes(1)).process(new OrderJoinFunction)

    result.print("matched")
    env.execute("orderJoinAndCheck")
  }

  class OrderJoinFunction extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
    override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      out.collect((left, right))
    }
  }

}
