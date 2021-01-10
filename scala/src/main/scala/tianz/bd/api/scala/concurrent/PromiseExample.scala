package tianz.bd.api.scala.concurrent

import scala.actors.threadpool.TimeoutException
import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success, Try}

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

  /**
   * future.onComplete(try=>promise.tryComplete(try)):
   * when future is completed,substitute the param of try with user-defined try,
   * to control the failure of promise manually
   *
   * @param step1
   * @param step2
   * @tparam T
   * @return
   */
  def cepPromise[T](step1: (Future[T], Long), step2: (Future[T], Long)): Future[T] = {
    val promiseAll = Promise[T]

    import tianz.bd.api.scala.concurrent.PromiseContext.timeoutFuture
    import tianz.bd.api.scala.concurrent.PromiseContext.CEPPromise

    timeoutProtect[T](step1._1, step1._2)(timeoutFuture[T]) transformTry {
      case s @ Success(value) =>
        //start step2
        timeoutProtect[T](step2._1, step2._2)(timeoutFuture[T]) transformTry {
          case s @ Success(value) =>
            s
          case f @ Failure(exception) =>
            promiseAll.failure(exception)
            f
        }
        s
      case f @ Failure(exception) =>
        promiseAll.failure(exception)
        f
    }

    promiseAll.future
  }
}

object PromiseContext {

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

  implicit class CEPPromise[T](var future: Future[T]) extends AnyRef {

    def transformTry(fun: Try[T] => Try[T]): Future[T] = {
      val promised = Promise[T]
      future onComplete (t => promised.tryComplete(fun(t)))
      promised.future
    }
  }
}
