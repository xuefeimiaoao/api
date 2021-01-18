package tianz.bd.api.scala.concurrent.asyncRefresh

import java.util
import java.util.concurrent.{ConcurrentHashMap, Executors}

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.FunSuite
import tianz.bd.api.scala.utils.HttpRestUtil

/**
 * @Author: Miaoxf
 * @Date: 2021/1/16 11:53
 * @Description:
 */
class HttpAsyncGroupTest extends FunSuite{

  private val ROUTE_FIELD: ConcurrentHashMap[String, util.List[String]] = new ConcurrentHashMap[String, util.List[String]]
  private val mapper: ObjectMapper = new ObjectMapper

  test("HttpAsyncGroupTest") {
    val service = Executors.newFixedThreadPool(500)
    while (true) {
      for (i <- Range(1,500)) {
        service.execute(new Runnable {
          override def run(): Unit = {
            if (i % 2 == 0) mock()
            else mock2()
          }
        })
      }
      Thread.sleep(2000)
      println(ROUTE_FIELD)
    }
  }

  def mock() {
    //调用返回一个List[String]
    val url = "http://127.0.0.1:8905/local/dsql/mock"
    if (ROUTE_FIELD.containsKey(url)){
      HttpRefreshUtil.coreRouteField(url, ClassType.ROUTE_FIELD, ROUTE_FIELD)
    }
    else {
      val meta = HttpRestUtil.get(url)
      val result = mapper.readValue(meta, classOf[util.List[String]])
      ROUTE_FIELD.put(url, result)
    }
  }

  def mock2() {
    //调用返回一个List[String]
    val url = "http://127.0.0.1:8905/local/dsql/mock2"
    if (ROUTE_FIELD.containsKey(url)){
      HttpRefreshUtil.coreRouteField(url, ClassType.ROUTE_FIELD, ROUTE_FIELD)
    }
    else {
      val meta = HttpRestUtil.get(url)
      val result = mapper.readValue(meta, classOf[util.List[String]])
      ROUTE_FIELD.put(url, result)
    }
  }


}
