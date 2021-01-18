package tianz.bd.api.scala.concurrent

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory

import scala.concurrent.{Future, Promise, TimeoutException}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal
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

    import tianz.bd.api.scala.concurrent.PromiseContext._

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

//  implicit var system = ActorSystem("PromiseUtils", ConfigFactory.load())

//  implicit def timeoutFuture[T](timeout: Long)(implicit system: ActorSystem): Future[T] = {
//    akka.pattern.after(FiniteDuration(timeout, TimeUnit.MILLISECONDS), system.scheduler)(Future.failed(throw new java.util.concurrent.TimeoutException(s"timeout for ${timeout} ms")))
//  }

  implicit def timeoutFuture[T](timeout: Long): Future[T] = {
    val promise = Promise[T]
    val media = Future {
      Thread.sleep(timeout)
    }

    media onComplete {
      case Success(value) =>
        promise.completeWith {
          try Future.failed {
            throw new TimeoutException(s"timeout for ${timeout} ms")
          }
          catch { case NonFatal(t) => Future.failed(t) }
        }
      case Failure(exception) => Future.failed(exception)
    }
    promise.future
  }

  implicit class CEPPromise[T](var future: Future[T]) extends AnyRef {

    def transformTry(fun: Try[T] => Try[T]): Future[T] = {
      val promised = Promise[T]
      future onComplete (t => promised.tryComplete(fun(t)))
      promised.future
    }
  }
}
