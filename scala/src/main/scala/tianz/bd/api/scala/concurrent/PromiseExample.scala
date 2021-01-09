package tianz.bd.api.scala.concurrent

import scala.actors.threadpool.TimeoutException
import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

/**
 * @Author: Miaoxf
 * @Date: 2021/1/9 15:53
 * @Description:
 */
object PromiseUtils {

  def firstFulfilled[T](future1: Future[T], future2: Future[T]): Future[T] = {
    val promise = Promise[T]()
    future1 onComplete promise.tryComplete
    future2 onComplete promise.tryComplete
    promise.future
  }

  def timeoutProtect[T](future: Future[T], timeout: Long)(implicit timeoutFuture: Long => Future[T]): Future[T] = {
    firstFulfilled[T](future, timeoutFuture(timeout))
  }

  def timeoutProtect[T](futureList: List[Future[T]], timeout: Long)(implicit timeoutFuture: Long => Future[T]): List[Future[T]] = {
    futureList map (future => timeoutProtect(future,timeout))
  }

  def timeoutFuture[T](timeout: Long): Future[T] = {
    def mock(): T = {
      val value = new Any
      value.asInstanceOf[T]
    }

    val future: Future[T] = Future[T] {
      Thread.sleep(timeout)
      mock()
    }
    future onComplete {
      case Success(value) => throw new TimeoutException()
      case Failure(exception) =>
    }
    future
  }
}
