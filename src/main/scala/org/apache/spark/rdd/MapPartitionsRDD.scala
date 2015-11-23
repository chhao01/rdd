package org.apache.spark.rdd

import scala.reflect.ClassTag

private[spark] class MapPartitionsRDD[U: ClassTag, T: ClassTag](
     prev: RDD[T],
     f: (Iterator[T]) => Iterator[U])
  extends RDD[U](prev) {
  override def compute(): Iterator[U] = f(prev.iterator())
}

