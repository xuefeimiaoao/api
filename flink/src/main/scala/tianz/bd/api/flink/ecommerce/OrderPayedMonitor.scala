package tianz.bd.api.flink.ecommerce

import java.io.File
import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time


/**
 * @Author: Miaoxf
 * @Date: 2021/1/3 15:56
 * @Description:
 *  基于CEP实现：
 *    如果订单超时(下单后15min内未支付)，输出到超时订单记录中
 *    如果订单支付成功，则返回成功。
 *
 */
object orderPayedMonitor {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //TODO 分布式环境下，会有多个wm，区别？

    val path = new File("").getCanonicalPath
    val orderCsv = env.readTextFile(path + "\\flink\\src\\main\\resources\\order\\orderLog.csv")
    val dataStream: DataStream[OrderEvent] = orderCsv.map(data => {
      val strings = data.split(",")
      OrderEvent(strings(0), strings(1), strings(3).toLong)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(3)) {
      override def extractTimestamp(t: OrderEvent): Long = t.orderTime
    })

    val rule = Pattern.begin[OrderEvent]("createOrder").where(_.orderType.equalsIgnoreCase("create"))
      .next("payOrder").where(_.orderType.equalsIgnoreCase("pay")).within(Time.minutes(15))

    val patternStream: PatternStream[OrderEvent] = CEP.pattern(dataStream.keyBy(0), rule)

    val timeoutTag: OutputTag[OrderResult] = new OutputTag[OrderResult]("order time out")
    val resultSteam: DataStream[OrderResult] = patternStream.select(timeoutTag, new OrderTimeoutFunction, new OrderSelectFunction)

    resultSteam.print("get payed")
    resultSteam.getSideOutput(timeoutTag).print("time out order")

    env.execute("orderTimeout")
  }


  class OrderTimeoutFunction extends PatternTimeoutFunction[OrderEvent, OrderResult] {
    override def timeout(pattern: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
      val id = pattern.get("createOrder").iterator().next().orderId
      OrderResult(id, "order time out")
    }
  }

  class OrderSelectFunction extends PatternSelectFunction[OrderEvent, OrderResult] {
    override def select(pattern: util.Map[String, util.List[OrderEvent]]): OrderResult = {
      val id = pattern.get("payOrder").iterator().next().orderId
      OrderResult(id, "order payed")
    }
  }

  //输入数据源的数据类型
  case class OrderEvent(var orderId: String, var orderType: String, var orderTime: Long)
  //输出数据的类型
  case class OrderResult(var orderId: String, var resultMsg: String)

  class OrderWaterMark(var maxLateTime: Long) extends AssignerWithPeriodicWatermarks[OrderEvent] {
    override def getCurrentWatermark: Watermark = {
      //这边不是样例类，所以只能new
      new Watermark(System.currentTimeMillis() - maxLateTime)
    }

    override def extractTimestamp(t: OrderEvent, l: Long): Long = {
      t.orderTime
    }
  }

}


