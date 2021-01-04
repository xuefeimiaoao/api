package tianz.bd.api.flink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

/**
 * @Author: Miaoxf
 * @Date: 2020/12/24 10:01
 */
object SimpleWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setBufferTimeout(0)
    val list = List("java", "java", "scala", "python", "python", "python")
    val text: DataStream[String] = env.fromCollection(list.toSeq)
    val result = text.map((_,1)).keyBy(0).sum(1)
    result.print()
    env.execute("simpleWC")
  }
}
