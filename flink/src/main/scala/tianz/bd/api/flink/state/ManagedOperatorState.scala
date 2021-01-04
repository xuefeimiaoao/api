package tianz.bd.api.flink.state

/**
 * @Author: Miaoxf
 * @Date: 2020/12/26 17:53
 */
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._
/**
 * managed operator state
 * all of which is stored as List format.
 * Because format of list is suitable for redistribution of state data.
 */
class CheckpointingCount() extends FlatMapFunction[(Int, Long), (Int, Long, Long)] with CheckpointedFunction {
  var operatorCount: Long = _
  var keyCount: ValueState[Long] = _
  //TODO 为什么用liststate?
  var operatorState: ListState[Long] = _

  override def flatMap(t: (Int, Long), collector: Collector[(Int, Long, Long)]): Unit = {
    operatorCount += 1
    val tmpCount = keyCount.value() + t._2
    keyCount.update(tmpCount)
    collector.collect(t._1, tmpCount, operatorCount)
  }

  override def snapshotState(functionSnapshotContext: FunctionSnapshotContext): Unit = {
    operatorState.clear()
    operatorState.add(operatorCount)
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    val valueStateDescriptor = new ValueStateDescriptor[Long]("keyCount", createTypeInformation[Long])
    val listStateDescriptor = new ListStateDescriptor[Long]("operatorCount", createTypeInformation[Long])
    val stateTtlConfig = StateTtlConfig
      .newBuilder(Time.seconds(100))
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      .build()
    valueStateDescriptor.enableTimeToLive(stateTtlConfig)
    listStateDescriptor.enableTimeToLive(stateTtlConfig)
    keyCount = context.getKeyedStateStore.getState(valueStateDescriptor)
    operatorState = context.getOperatorStateStore.getListState(listStateDescriptor)
//    operatorState = context.getUnionListState()
    if (context.isRestored) {
      operatorCount = operatorState.get().asScala.sum
    }
  }
}


object CountSum {
  import org.apache.flink.api.scala._
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env
    env.fromCollection(List(
      (39, 1L),
      (39, 2L),
      (100, 5L)
    )).keyBy(0).flatMap(new CheckpointingCount).print()
    env.execute()
  }
}
