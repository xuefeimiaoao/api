package tianz.bd.api.scala.concurrent.asyncRefresh

import java.util
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ExecutorService, Executors}

import org.apache.commons.lang.StringUtils

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}


/**
 * @Author: Miaoxf
 * @Date: 2021/1/8 10:10
 * @Description:
 */
class HttpAsyncGroup {

  val lock = new ReentrantLock()
  val notEmpty = lock.newCondition()
  val notStart = lock.newCondition()
  var timeFilter: util.List[AsyncTask] = _
  var producerService: ExecutorService = _
  var consumerService: ExecutorService = _
  var state: java.util.HashMap[String, Long] = _

  var scheduleRate: Long = 30*1000L
  val startDelay: Long = 10*60*1000L
  val CURRENTTIME = "currentTime"

  /**
   * schedule the request of core-Api, and execute asynchronous request which is protected by timeout Exception.
   *
   * @param size size of queue
   * @param scheduleRate the rate of requesting interface
   */
  def this(size: Int, scheduleRate: Long) {
    this()
    this.timeFilter = new util.ArrayList[AsyncTask](size)
    this.scheduleRate = scheduleRate
    this.state = new java.util.HashMap[String, Long]()
    this.producerService = Executors.newSingleThreadExecutor()
    this.consumerService = Executors.newSingleThreadExecutor()
    lazyStart()
  }

  def schedule(): Unit = {
    //TODO 持续优化，防止任务堆积（生产者-消费者，异常，速度不一致）
    //protect quote caught by closure
    //control the time of callback, but it seems meanless here
    while (true) {
      try {
        this.lock.lock()
        notStart.signal()
        val index = timeFilter.size()-1
        if (index >= 0) {
          val task: AsyncTask = timeFilter.get(index)
          var result = 0
          if (task != null) {
            try {
              result = task.execute()
              //invoke callback function for one time
              timeFilter.remove(index)
            }
            catch { case e: Exception => }
          }
        } else {
          notEmpty.await()
        }
      } finally {
        this.lock.unlock()
      }
    }
  }

  def lazyStart() = {
    val wait = Future.apply(Thread.sleep(startDelay))
    wait onComplete {
      case Success(value) =>
        consumerService.submit(new Runnable {
          override def run(): Unit = schedule()
        })
      case Failure(exception) => throw new RuntimeException("HttpAsyncGroup启动失败!")
    }
  }

  def tryPost[T](asyncTask: AsyncTask): Unit = {
    try {
      producerService.submit(new Runnable {
        override def run(): Unit = syncPost(asyncTask)
      })
    } catch {
      case e: Exception =>
    }
  }

  def syncPost[T](asyncTask: AsyncTask): Unit = {
    def shouldPost(lastTime: Long, taskTime: String): Boolean = {
      if ((taskTime.toLong - lastTime) > scheduleRate) {
        true
      } else {
        false
      }
    }

    var taskTime = asyncTask.options.get(CURRENTTIME)
    if (StringUtils.isBlank(taskTime)) {
      taskTime = System.currentTimeMillis() + ""
    }

    val lastTime = state.get(asyncTask.taskId)
    if (lastTime != null && !shouldPost(lastTime, taskTime)) {
      //TODO 这边有很多对象?
    } else {
      //TODO 如果FutureTask失败，那么state.remove(asyncTask.taskId)
      try {
        this.lock.lock()
        while (state.size() == 0) {
          //确保state不被清空
          notStart.await()
          state.put("", 0L)
        }
        val lastTime = state.get(asyncTask.taskId)
        if (lastTime != null && !shouldPost(lastTime, taskTime)) return

        asyncTask.start()

        //采用系统当前时间，而非taskTime，是因为，当前时间可以过滤更多无意义的任务
        state.put(asyncTask.taskId, System.currentTimeMillis())
        //如果满了，会自动扩容，不会阻塞
        timeFilter.add(asyncTask)
        notEmpty.signal()
      } finally {
        this.lock.unlock()
      }
    }
  }
}
