package org.apache.spark.rdd

import org.apache.spark._

import scala.reflect.ClassTag

class UnionRDD[T: ClassTag](sc: SparkContext, var rdds: Seq[RDD[T]]) extends RDD[T](sc) {
  override def compute(): Iterator[T] = {
    rdds.map(_.iterator()).reduce(_ ++ _)
  }
}