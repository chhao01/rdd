/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rdd

import java.io.{FileWriter, BufferedWriter, ObjectOutputStream, FileOutputStream}

import org.apache.spark._
import org.apache.spark.util.Utils
import scala.language.implicitConversions
import scala.reflect.ClassTag

abstract class RDD[T: ClassTag](
    @transient private val sc: SparkContext
  ) extends Serializable with Logging {

  /** Construct an RDD with just a one-to-one dependency on one parent */
  def this(@transient oneParent: RDD[_]) = this(oneParent.context)

  private[spark] def conf = context.conf
  // =======================================================================
  // Methods that should be implemented by subclasses of RDD
  // =======================================================================

  def compute(): Iterator[T]

  final def iterator(): Iterator[T] = {
    compute()
  }

  // =======================================================================
  // Methods and fields available on all RDDs
  // =======================================================================
  def context: SparkContext = sc

  private[spark] def withScope[U](body: => U): U = RDDOperationScope.withScope[U](sc)(body)

  /** A unique ID for this RDD (within its SparkContext). */
  val id: Int = SparkContext.newRddId()

  /** A friendly name for this RDD */
  @transient var name: String = null

  /** Assign a name to this RDD */
  def setName(_name: String): this.type = {
    name = _name
    this
  }

  // Transformations (return a new RDD)

  def union(other: RDD[T]): RDD[T] = withScope {
    new UnionRDD(sc, Array(this, other))
  }

  /**
   * Return the union of this RDD and another one. Any identical elements will appear multiple
   * times (use `.distinct()` to eliminate them).
   */
  def ++(other: RDD[T]): RDD[T] = withScope {
    this.union(other)
  }

  /**
   * Return a new RDD by applying a function to all elements of this RDD.
   */
  def map[U: ClassTag](f: T => U): RDD[U] = withScope {
    new MapPartitionsRDD[U, T](this, (iter) => iter.map(f))
  }

  /**
   *  Return a new RDD by first applying a function to all elements of this
   *  RDD, and then flattening the results.
   */
  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] = withScope {
    new MapPartitionsRDD[U, T](this, (iter) => iter.flatMap(f))
  }

  /**
   * Return a new RDD containing only the elements that satisfy a predicate.
   */
  def filter(f: T => Boolean): RDD[T] = withScope {
    new MapPartitionsRDD[T, T](
      this,
      (iter) => iter.filter(f))
  }

  /**
   * Return the Cartesian product of this RDD and another one, that is, the RDD of all pairs of
   * elements (a, b) where a is in `this` and b is in `other`.
   */
  def cartesian[U: ClassTag](other: RDD[U]): RDD[(T, U)] = withScope {
    new CartesianRDD(sc, this, other)
  }

  /**
   * Return an RDD of grouped items. Each group consists of a key and a sequence of elements
   * mapping to that key. The ordering of elements within each group is not guaranteed, and
   * may even differ each time the resulting RDD is evaluated.
   *
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using [[PairRDDFunctions.aggregateByKey]]
   * or [[PairRDDFunctions.reduceByKey]] will provide much better performance.
   */
  def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = withScope {
    this.map(t => (f(t), t)).groupByKey()
  }

  /**
   * Return a new RDD by applying a function to each partition of this RDD.
   *
   * `preservesPartitioning` indicates whether the input function preserves the partitioner, which
   * should be `false` unless this is a pair RDD and the input function doesn't modify the keys.
   */
  def mapPartitions[U: ClassTag](
      f: Iterator[T] => Iterator[U]): RDD[U] = withScope {
    new MapPartitionsRDD(
      this,
      (iter: Iterator[T]) => f(iter))
  }

  // Actions (launch a job to return a value to the user program)

  /**
   * Applies a function f to all elements of this RDD.
   */
  def foreach(f: T => Unit): Unit = withScope {
    sc.runJob(this, (iter: Iterator[T]) => iter.foreach(f))
  }

  /**
   * Applies a function f to each partition of this RDD.
   */
  def foreachPartition(f: Iterator[T] => Unit): Unit = withScope {
    sc.runJob(this, (iter: Iterator[T]) => f(iter))
  }

  /**
   * Return an array that contains all of the elements in this RDD.
   */
  def collect(): Array[T] = withScope {
    val results = sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
    Array.concat(results: _*)
  }

  /**
   * Return an RDD that contains all matching values by applying `f`.
   */
  def collect[U: ClassTag](f: PartialFunction[T, U]): RDD[U] = withScope {
    filter(f.isDefinedAt).map(f)
  }

  /**
   * Return the number of elements in the RDD.
   */
  def count(): Long = sc.runJob(this, Utils.getIteratorSize _).sum

  /**
   * Take the first num elements of the RDD. It works by first scanning one partition, and use the
   * results from that partition to estimate the number of additional partitions needed to satisfy
   * the limit.
   *
   * @note due to complications in the internal implementation, this method will raise
   * an exception if called on an RDD of `Nothing` or `Null`.
   */
  def take(num: Int): Array[T] = withScope {
    this.collect().take(num)
  }

  /**
   * Creates tuples of the elements in this RDD by applying `f`.
   */
  def keyBy[K](f: T => K): RDD[(K, T)] = withScope {
    map(x => (f(x), x))
  }

  /**
   * Reduces the elements of this RDD using the specified commutative and
   * associative binary operator.
   */
  def reduce(f: (T, T) => T): T = withScope {
    val reducePartition: Iterator[T] => Option[T] = iter => {
      if (iter.hasNext) {
        Some(iter.reduceLeft(f))
      } else {
        None
      }
    }

    val mergeResult = (l: Option[T], r: Option[T]) => (l, r) match {
      case (Some(a), Some(b)) => Some(f(a, b))
      case (None, b) => b
      case (a, None) => a
      case _ => None
    }

    sc.runJob(this, reducePartition).reduce(mergeResult).getOrElse(throw new UnsupportedOperationException("empty collection"))
  }

  override def toString: String = "%s%s[%d]".format(
    Option(name).map(_ + " ").getOrElse(""), getClass.getSimpleName, id)
}


/**
 * Defines implicit functions that provide extra functionalities on RDDs of specific types.
 */
object RDD {
  implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] = {
    new PairRDDFunctions(rdd)
  }
}
