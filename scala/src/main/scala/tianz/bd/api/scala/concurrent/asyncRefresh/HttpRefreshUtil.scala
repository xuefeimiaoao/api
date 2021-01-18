package tianz.bd.api.scala.concurrent.asyncRefresh

import java.util._
import java.util.concurrent.ConcurrentHashMap

import com.fasterxml.jackson.databind.ObjectMapper
import tianz.bd.api.scala.concurrent.asyncRefresh.ClassType.ClassType


/**
 * @Author: Miaoxf
 * @Date: 2021/1/11 12:50
 * @Description:
 */
object HttpRefreshUtil {

  private val scheduleRate: Long = 30*1000L
  private val asycGroup = new HttpAsyncGroup(20, scheduleRate)
  private val mapper: ObjectMapper = new ObjectMapper
  private val state: ConcurrentHashMap[String, Long] = new ConcurrentHashMap()


  def coreRouteField(url: String, classType: ClassType, routeField: ConcurrentHashMap[String, List[String]]) = {
    val startTime = System.currentTimeMillis()
    //sync task
    val result = routeField.get(url)

    val taskTime = System.currentTimeMillis()
    if (prePost(taskTime, url)) {
      //async task
      val options: HashMap[String, String] = new HashMap[String, String]
      options.put("url", url)
      options.put("currentTime", taskTime + "")
      state.put(url, taskTime)
      asycGroup.tryPost(new RouteFieldTask(options, url, classType, routeField, mapper))
    }

    val endTime = System.currentTimeMillis()
    logUtil(endTime - startTime, "HttpRefreshUtil.coreRouteField: cost ")

    result
  }

  def coreLocalMeta(url: String, classType: ClassType, localMeta: ConcurrentHashMap[String, InnerMetaDataBaseInfo]) = {
    val startTime = System.currentTimeMillis()
    //sync task
    val result: InnerMetaDataBaseInfo = localMeta.get(url)

    val taskTime = System.currentTimeMillis()
    //proPost，防止内存泄漏
    if (prePost(taskTime, url)) {
      //async task
      val options: HashMap[String, String] = new HashMap[String, String]
      options.put("url", url)
      options.put("currentTime", taskTime + "")
      state.put(url, taskTime)
      asycGroup.tryPost(new LocalMetaAsyncTask(options, url, classType, localMeta, mapper))
    }

    val endTime = System.currentTimeMillis()
    logUtil(endTime - startTime, "HttpRefreshUtil.coreLocalMeta: cost ")

    result
  }

  def prePost(currentTime: Long, taskId: String) = {
    val lastTime = state.get(taskId)
    if (lastTime == null || (currentTime - lastTime) >= scheduleRate) {
      true
    } else {
      false
    }
  }

  def logUtil(loss: Long, log: String) = {
//    if ( loss >= 10 && loss < 50) {
//      logDebug(log + (loss + "ms"))
//    } else if (loss >= 50 && loss < 100) {
//      logWarning(log + (loss + "ms"))
//    } else if (loss >= 100) {
//      logError(log + (loss + "ms"))
//    }
  }
}
