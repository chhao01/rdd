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

import scala.collection.Map
import scala.reflect.ClassTag
import scala.util.DynamicVariable

import org.apache.spark._
import org.apache.spark.util.collection.CompactBuffer

private[spark] class PairRDDFunctions[K, V](self: RDD[(K, V)])
                            (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
  extends Logging
  with Serializable
{

  /**
   * :: Experimental ::
   * Generic function to combine the elements for each key using a custom set of aggregation
   * functions. Turns an RDD[(K, V)] into a result of type RDD[(K, C)], for a "combined type" C
   * Note that V and C can be different -- for example, one might group an RDD of type
   * (Int, Int) into an RDD of type (Int, Seq[Int]). Users provide three functions:
   *
   * - `createCombiner`, which turns a V into a C (e.g., creates a one-element list)
   * - `mergeValue`, to merge a V into a C (e.g., adds it to the end of a list)
   * - `mergeCombiners`, to combine two C's into a single one.
   *
   * In addition, users can control the partitioning of the output RDD, and whether to perform
   * map-side aggregation (if a mapper can produce multiple items with the same key).
   */
  def combineByKeyWithClassTag[C](
                                   createCombiner: V => C,
                                   mergeValue: (C, V) => C,
                                   mergeCombiners: (C, C) => C)(implicit ct: ClassTag[C]): RDD[(K, C)] = self.withScope {
    require(mergeCombiners != null, "mergeCombiners must be defined")

    self.mapPartitions(iter => {
      iter.foldLeft(Map[K, C]()){ case (m, entry) =>
        if (m.contains(entry._1)) {
          m + ((entry._1, mergeValue(m(entry._1), entry._2)))
        } else {
          m + ((entry._1, createCombiner(entry._2)))
        }
      }.toIterator
    })
  }

  /**
   * Generic function to combine the elements for each key using a custom set of aggregation
   * functions. This method is here for backward compatibility. It does not provide combiner
   * classtag information to the shuffle.
   *
   * @see [[combineByKeyWithClassTag]]
   */
  def combineByKey[C](
                       createCombiner: V => C,
                       mergeValue: (C, V) => C,
                       mergeCombiners: (C, C) => C): RDD[(K, C)] = self.withScope {
    combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners)(null)
  }

  def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,
                                                combOp: (U, U) => U): RDD[(K, U)] = self.withScope {
    combineByKeyWithClassTag[U]((v: V) => seqOp(zeroValue, v), seqOp, combOp)
  }

  /**
   * Merge the values for each key using an associative function and a neutral "zero value" which
   * may be added to the result an arbitrary number of times, and must not change the result
   * (e.g., Nil for list concatenation, 0 for addition, or 1 for multiplication.).
   */
  def foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)] = self.withScope {
    combineByKeyWithClassTag[V]((v: V) => func(zeroValue, v), func, func)
  }

  /**
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce.
   */
  def reduceByKey(func: (V, V) => V): RDD[(K, V)] = self.withScope {
    combineByKeyWithClassTag[V]((v: V) => v, func, func)
  }

  /**
   * Count the number of elements for each key, collecting the results to a local Map.
   *
   * Note that this method should only be used if the resulting map is expected to be small, as
   * the whole thing is loaded into the driver's memory.
   * To handle very large results, consider using rdd.mapValues(_ => 1L).reduceByKey(_ + _), which
   * returns an RDD[T, Long] instead of a map.
   */
  def countByKey(): Map[K, Long] = self.withScope {
    self.mapValues(_ => 1L).reduceByKey(_ + _).collect().toMap
  }

  /**
   * Pass each value in the key-value pair RDD through a map function without changing the keys;
   * this also retains the original RDD's partitioning.
   */
  def mapValues[U](f: V => U): RDD[(K, U)] = self.withScope {
    new MapPartitionsRDD[(K, U), (K, V)](self,
      (iter) => iter.map { case (k, v) => (k, f(v)) })
  }

  /**
   * Group the values for each key in the RDD into a single sequence. Allows controlling the
   * partitioning of the resulting key-value pair RDD by passing a Partitioner.
   * The ordering of elements within each group is not guaranteed, and may even differ
   * each time the resulting RDD is evaluated.
   *
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using [[PairRDDFunctions.aggregateByKey]]
   * or [[PairRDDFunctions.reduceByKey]] will provide much better performance.
   *
   * Note: As currently implemented, groupByKey must be able to hold all the key-value pairs for any
   * key in memory. If a key has too many values, it can result in an [[OutOfMemoryError]].
   */
  def groupByKey(): RDD[(K, Iterable[V])] = self.withScope {
    // groupByKey shouldn't use map side combine because map side combine does not
    // reduce the amount of data shuffled and requires all map side data be inserted
    // into a hash table, leading to more objects in the old gen.
    val createCombiner = (v: V) => CompactBuffer(v)
    val mergeValue = (buf: CompactBuffer[V], v: V) => buf += v
    val mergeCombiners = (c1: CompactBuffer[V], c2: CompactBuffer[V]) => c1 ++= c2
    val bufs = combineByKeyWithClassTag[CompactBuffer[V]](
      createCombiner, mergeValue, mergeCombiners)
    bufs.asInstanceOf[RDD[(K, Iterable[V])]]
  }

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Uses the given Partitioner to partition the output RDD.
   */
  def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))] = self.withScope {
    this.cogroup(other).flatMapValues( pair =>
      for (v <- pair._1.iterator; w <- pair._2.iterator) yield (v, w)
    )
  }

  /**
   * For each key k in `this` or `other1` or `other2` or `other3`,
   * return a resulting RDD that contains a tuple with the list of values
   * for that key in `this`, `other1`, `other2` and `other3`.
   */
  def cogroup[W1, W2, W3](other1: RDD[(K, W1)],
                          other2: RDD[(K, W2)],
                          other3: RDD[(K, W3)])
  : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = self.withScope {
    val cg = new CoGroupedRDD[K](Seq(self, other1, other2, other3))
    cg.mapValues { case Seq(vs, w1s, w2s, w3s) =>
      (vs.asInstanceOf[Iterable[V]],
        w1s.asInstanceOf[Iterable[W1]],
        w2s.asInstanceOf[Iterable[W2]],
        w3s.asInstanceOf[Iterable[W3]])
    }
  }

  def cogroup[W1](other1: RDD[(K, W1)])
  : RDD[(K, (Iterable[V], Iterable[W1]))] = self.withScope {
    val cg = new CoGroupedRDD[K](Seq(self, other1))
    cg.mapValues { case Seq(vs, w1s) =>
      (vs.asInstanceOf[Iterable[V]],
        w1s.asInstanceOf[Iterable[W1]])
    }
  }

  def cogroup[W1, W2](other1: RDD[(K, W1)],
                          other2: RDD[(K, W2)])
  : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = self.withScope {
    val cg = new CoGroupedRDD[K](Seq(self, other1, other2))
    cg.mapValues { case Seq(vs, w1s, w2s) =>
      (vs.asInstanceOf[Iterable[V]],
        w1s.asInstanceOf[Iterable[W1]],
        w2s.asInstanceOf[Iterable[W2]])
    }
  }

  /**
   * Pass each value in the key-value pair RDD through a flatMap function without changing the
   * keys; this also retains the original RDD's partitioning.
   */
  def flatMapValues[U](f: V => TraversableOnce[U]): RDD[(K, U)] = self.withScope {
    new MapPartitionsRDD[(K, U), (K, V)](self,
      (iter) => iter.flatMap { case (k, v) =>
        f(v).map(x => (k, x))
      })
  }

  /** Alias for cogroup. */
  def groupWith[W](other: RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))] = self.withScope {
    cogroup(other)
  }

  /** Alias for cogroup. */
  def groupWith[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)])
  : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = self.withScope {
    cogroup(other1, other2)
  }

  /** Alias for cogroup. */
  def groupWith[W1, W2, W3](other1: RDD[(K, W1)], other2: RDD[(K, W2)], other3: RDD[(K, W3)])
  : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = self.withScope {
    cogroup(other1, other2, other3)
  }

  /**
   * Return the list of values in the RDD for key `key`. This operation is done efficiently if the
   * RDD has a known partitioner by only searching the partition that the key maps to.
   */
  def lookup(key: K): Seq[V] = self.withScope {
    self.filter(_._1 == key).map(_._2).collect()
  }

  /**
   * Return an RDD with the keys of each tuple.
   */
  def keys: RDD[K] = self.map(_._1)

  /**
   * Return an RDD with the values of each tuple.
   */
  def values: RDD[V] = self.map(_._2)
}

private[spark] object PairRDDFunctions {
  val RECORDS_BETWEEN_BYTES_WRITTEN_METRIC_UPDATES = 256

  /**
   * Allows for the `spark.hadoop.validateOutputSpecs` checks to be disabled on a case-by-case
   * basis; see SPARK-4835 for more details.
   */
  val disableOutputSpecValidation: DynamicVariable[Boolean] = new DynamicVariable[Boolean](false)
}
