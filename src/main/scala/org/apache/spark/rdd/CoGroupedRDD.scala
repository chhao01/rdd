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

import scala.collection.mutable
import scala.language.existentials

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.util.collection.CompactBuffer

class CoGroupedRDD[K: ClassTag](
    @transient var rdds: Seq[RDD[_ <: Product2[K, _]]])
  extends RDD[(K, Seq[Iterable[_]])](rdds.head.context) {

  private type CoGroup = CompactBuffer[Any]
  private type CoGroupValue = (Any, Int)  // Int is the idx of the rdd in the cogroup
  private type CoGroupCombiner = Seq[CoGroup]

  override def compute(): Iterator[(K, Seq[Iterable[_]])] = {
    val numRdds = rdds.length

    val rddIterators = new ArrayBuffer[(Iterator[Product2[K, Any]], Int)]
    for ((rdd, depNum) <- rdds.zipWithIndex) {
      rddIterators += ((rdd.compute(), depNum))
    }

    val map = new mutable.HashMap[K, CoGroupCombiner]()

    val createCombiner: (() => CoGroupCombiner) = () => {
      val newCombiner = Seq.fill(numRdds)(new CoGroup)
      newCombiner
    }
    val mergeValue: (CoGroupCombiner, CoGroupValue) => CoGroupCombiner =
      (combiner, value) => {
        combiner(value._2) += value._1
        combiner
      }

    for ((it, rddIdx) <- rddIterators) {
      it.foreach { pair =>
        mergeValue(map.getOrElseUpdate(pair._1, createCombiner()), new CoGroupValue(pair._2, rddIdx))
      }
    }

    map.iterator
  }
}
