package tianz.bd.api.scala.concurrent.asyncRefresh

import java.util.concurrent.ConcurrentHashMap

import com.fasterxml.jackson.databind.ObjectMapper
import tianz.bd.api.scala.concurrent.asyncRefresh.ClassType.ClassType
import tianz.bd.api.scala.utils.HttpRestUtil

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
 * @Author: Miaoxf
 * @Date: 2021/1/14 10:47
 * @Description:
 */
case class result(var r: Int = -1)

class LocalMetaAsyncTask(override var options: java.util.Map[String, String],
                         override var taskId: String,
                         override var classType: ClassType,
                         override var quote: Object,
                         var mapper: ObjectMapper) extends AsyncTask {
  var innerValue: InnerMetaDataBaseInfo = _
  var future: Future[InnerMetaDataBaseInfo] = _

  override def execute(): Int = {
    def callback(r: result): Unit = {
      val target = quote.asInstanceOf[ConcurrentHashMap[String, InnerMetaDataBaseInfo]]
      target.put(this.taskId, innerValue)
      r.r = 1
    }
    def fail(r: result) = r.r = 0
    val r: result = result()
    future.onComplete {
      case Success(value) =>
        //TODO 这边错误
        innerValue = value.asInstanceOf[InnerMetaDataBaseInfo]
        callback(r)
      case Failure(exception) => fail(r)
    }
    r.r
  }

  override def start(): Future[InnerMetaDataBaseInfo] = {
    val future1: Future[InnerMetaDataBaseInfo] = Future[InnerMetaDataBaseInfo] {
      val meta = HttpRestUtil.get(options.get("url"))
      mapper.readValue(meta, classOf[InnerMetaDataBaseInfo])
    }
    val result = PromiseUtils.timeoutProtect[InnerMetaDataBaseInfo](future1, 20*1000L)
    future = result
    result
  }

}
