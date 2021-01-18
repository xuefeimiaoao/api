package tianz.bd.api.scala.concurrent.asyncRefresh

import java.util.concurrent.BlockingQueue


/**
 * @Author: Miaoxf
 * @Date: 2021/1/7 12:54
 * @Description:
 */
trait AsyncRefresh[T] {

  //可以按照时间排序和过滤
  var timeFilter: BlockingQueue[AsyncTask]

  def post[T](asyncTask: AsyncTask)
}
