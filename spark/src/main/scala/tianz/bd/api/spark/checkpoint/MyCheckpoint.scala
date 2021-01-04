package tianz.bd.api.spark.checkpoint

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: Miaoxf
 * @Date: 2020/12/24 11:27
 */
class StreamingCheckpoint {

  /**
   * 如果是首次启动，将创建一个新的StreamingContext,
   * 如果是application失败后，重启时，如果有checkpoint，则从checkpoint中恢复。
   * @return
   */
  def createContext(): StreamingContext = {
    val conf = new SparkConf().setAppName("StreamingCheckpoint").setMaster("local[10]")
    val sc = new StreamingContext(conf, Seconds(2))
    sc.checkpoint("")
    sc
  }

  def main(args: Array[String]): Unit = {
    val sc = StreamingContext.getOrCreate("", () => createContext())
    sc.start()
    sc.awaitTermination()
  }

}

class BatchCheckpoint {

}
