package tianz.bd.api.scala.concurrent.asyncRefresh

import java.util
import java.util.concurrent.ConcurrentHashMap

/**
 * @Author: Miaoxf
 * @Date: 2021/1/8 10:36
 * @Description:
 */
object ClassType extends Enumeration {

  type ClassType = Value

  val LOCAL_META, ROUTE_FIELD, ROUTEVALUEMAP, ANYDSQLCONFIG = Value

  def cast(target: ClassType, value: java.io.Serializable) = {
    target match {
      case LOCAL_META =>
        value.asInstanceOf[ConcurrentHashMap[String, InnerMetaDataBaseInfo]]
      case ROUTE_FIELD =>
        value.asInstanceOf[ConcurrentHashMap[String, util.List[String]]]
      case ROUTEVALUEMAP =>
        value.asInstanceOf[util.IdentityHashMap[String, DsqlConfig]]
      case ANYDSQLCONFIG =>
        value.asInstanceOf[DsqlConfig]
    }
  }
}
