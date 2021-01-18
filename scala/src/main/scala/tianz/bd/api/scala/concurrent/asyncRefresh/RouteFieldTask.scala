package tianz.bd.api.scala.concurrent.asyncRefresh

import java.util
import java.util.concurrent.ConcurrentHashMap

import com.fasterxml.jackson.databind.ObjectMapper
import tianz.bd.api.scala.concurrent.asyncRefresh.ClassType.ClassType
import tianz.bd.api.scala.utils.HttpRestUtil

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
 * @Author: Miaoxf
 * @Date: 2021/1/14 14:29
 * @Description:
 */
class RouteFieldTask(override var options: java.util.Map[String, String],
                     override var taskId: String,
                     override var classType: ClassType,
                     override var quote: Object,
                     var mapper: ObjectMapper) extends AsyncTask {
  var innerValue: util.List[String] = _
  var future: Future[util.List[String]] = _

  override def start(): Future[util.List[String]] = {
    val future1: Future[util.List[String]] = Future[util.List[String]] {
      val meta = HttpRestUtil.get(options.get("url"))
      mapper.readValue(meta, classOf[util.List[String]])
    }
    val result = PromiseUtils.timeoutProtect[util.List[String]](future1, 20*1000L)
    future = result
    result
  }

  override def execute(): Int = {
    def callback(r: result): Unit = {
      val target = quote.asInstanceOf[ConcurrentHashMap[String, util.List[String]]]
      target.put(this.taskId, innerValue)
      r.r = 1
    }
    def fail(r: result) = r.r = 0
    var r = result()
    future.onComplete {
      case Success(value) =>
        //TODO 这边错误
        innerValue = value
        callback(r)
      case Failure(exception) => fail(r)
    }
    r.r
  }

}
