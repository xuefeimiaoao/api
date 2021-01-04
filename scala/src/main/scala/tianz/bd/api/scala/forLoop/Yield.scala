package tianz.bd.api.scala.forLoop

/**
 * @Author: Miaoxf
 * @Date: 2020/12/31 12:52
 */
class Yield {

}

/**
 * yield返回集合的类型与被遍历的集合类型是一致的
 */
object Yield {

  def main(args: Array[String]): Unit = {
    val ints = for (i <- 1 to 5) yield i
    println(ints)

    val range = for (i <- Range(1, 3) if i > 2) yield i * 2
    println(range)

    def result =
      for {
        i <- Range(1, 10)
        if (i > 3)
      } yield i % 2
    println(result)

  }

}

