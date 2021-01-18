package tianz.bd.api.scala.concurrent.asyncRefresh

import tianz.bd.api.scala.concurrent.asyncRefresh.ClassType.ClassType

import scala.concurrent.Future
import scala.reflect.ClassTag


/**
 * @Author: Miaoxf
 * @Date: 2021/1/8 9:47
 * @Description:
 */
abstract class AsyncTask() {
  var options: java.util.Map[String, String]
  var taskId: String
  var classType: ClassType
  var quote: Object
  //TODO 如果futureTask没有进入队列，则销毁

  def start(): Future[Any]
  def execute(): Int
}

abstract class HttpAsyncTask(override var options: java.util.Map[String, String],
                             override var taskId: String,
                             override var classType: ClassType,
                             override var quote: Object) extends AsyncTask {

//  var target: java.io.Serializable = _
//  var innerValue: java.io.Serializable = _
//  var future: Future[java.io.Serializable] = _

  def this() {
    this(null, null, null, null)
  }


  override def execute(): Int /*= {
    future.onComplete {
      case Success(value) =>
        //TODO 这边错误
        target = ClassType.cast(classType, value)
        innerValue = value
        callback()
        return 1
      case Failure(exception) =>
        throw new RuntimeException(exception)
    }
    -1
  }*/

  def start(): Future[java.io.Serializable] /*= {
    val result = PromiseUtils.timeoutProtect[java.io.Serializable](Future(operate()), 20*1000L)
    future = result
    result
  }*/

  @throws(classOf[java.lang.Exception])
  def operate[OUT: ClassTag](): OUT
}



