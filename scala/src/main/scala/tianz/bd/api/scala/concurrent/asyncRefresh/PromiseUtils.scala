package tianz.bd.api.scala.concurrent.asyncRefresh

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise, TimeoutException}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}


/**
 * @Author: Miaoxf
 * @Date: 2021/1/11 9:10
 * @Description:
 */
object PromiseUtils {

  def firstFulfilled[T](future: Future[T], future2: Future[T]): Future[T] = {
    val promise = Promise[T]
    future onComplete promise.tryComplete
    future2 onComplete promise.tryComplete
    promise.future
  }

  def timeoutProtect[T](future: Future[T], timeout: Long): Future[T] = {
    firstFulfilled(future, timeoutFuture(timeout))
  }

  def timeoutFuture[T](timeout: Long): Future[T] = {
    val promise = Promise[T]
    val media = Future {
      Thread.sleep(timeout)
    }

    media onComplete {
      case Success(value) =>
        promise.completeWith {
          try Future.failed {
            throw new TimeoutException(s"timeout for ${timeout} ms!")
          }
          catch { case NonFatal(t) => Future.failed(t) }
        }
      case Failure(exception) => Future.failed(exception)
    }
    promise.future
  }
}
