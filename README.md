基于Scala实现的完全模拟Spark RDD函数接口的代码.
```scala
package example

import org.apache.spark.SparkContext

object Example {
  def main(args: Array[String]) {
    val sc = SparkContext("local[4]", "test")
    println(sc.makeRDD(1 to 100).filter(_ % 2 == 1).count())
    sc.parallelize(1 to 30).groupBy(_ % 5).map { case (k, it) =>
      s"k => sum: ${it.sum}"
    }.foreach(println)
  }
}
```

这个工程基于Apache LICENSE-2.0 协议，可以自由传播，重在学习Scala编程，从零实现Spark代码。

欢迎提交Pull Request，完善代码注释以及多线程执行。

联系人：Cheng Hao

email:chhao01@gmail.com
qq:16426021
qq群:240348813

