package tianz.bd.api.flink.state

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector


/**
 * @Author: Miaoxf
 * @Date: 2020/12/25 10:54
 */

/**
 * managed keyed state: ValueState,
 * which is a special operator state binding with an operator and a key simultaneously
 */
class CountWindowAverage extends RichFlatMapFunction[(Long, Double), (Long, Double)] {
  private var sum: ValueState[(Long, Double)] = _
  override def flatMap(input: (Long, Double), out: Collector[(Long, Double)]): Unit = {
    // access the state value
    val tmpCurrentSum = sum.value
    // If it hasn't been used before, it will be null
    val currentSum = if (tmpCurrentSum != null) {
      tmpCurrentSum
    } else {
      (0L, 0d)
    }
    // update the count
    val newSum = (currentSum._1 + 1, currentSum._2 + input._2)
    // update the state
    sum.update(newSum)
    // if the count reaches 2, emit the average and clear the state
    if (newSum._1 >= 2) {
      out.collect((input._1, newSum._2 / newSum._1))
      //将状态清除
      //sum.clear()
    }
  }
  override def open(parameters: Configuration): Unit = {
    val stateTtlConfig = StateTtlConfig
      .newBuilder(Time.seconds(10))
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      .build()

    val valueStateDescriptor = new ValueStateDescriptor[(Long, Double)]("average", createTypeInformation[(Long, Double)])
    valueStateDescriptor.enableTimeToLive(stateTtlConfig)
    sum = getRuntimeContext.getState(
      valueStateDescriptor
    )
  }
}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
object ECountWindowAverage {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.fromCollection(List(
      (1L, 3d),
      (1L, 5d),
      (1L, 7d),
      (1L, 4d),
      (1L, 2d)
    )).keyBy(_._1)
      .flatMap(new CountWindowAverage())
      .print()

    /*.keyBy(_._1)
      .flatMap(new CountWindowAverage())
      .print()*/

    // the printed output will be (1,4) and (1,5)
    env.execute("ExampleManagedState")
  }
}

