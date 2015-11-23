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

package org.apache.spark

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.rdd._

import scala.language.implicitConversions
import scala.reflect.ClassTag

class SparkContext(config: SparkConf) extends Logging {
  def this() = this(new SparkConf())

  logInfo(s"Running Spark version ${SparkContext.SPARK_VERSION}")

  private[spark] def conf: SparkConf = this.config

  def master: String = conf.get("spark.master")
  def appName: String = conf.get("spark.app.name")

  private[spark] def withScope[U](body: => U): U = RDDOperationScope.withScope[U](this)(body)

  // Methods for creating RDDs
  def parallelize[T: ClassTag](
      seq: Seq[T]): RDD[T] = withScope {
    new ParallelCollectionRDD[T](this, seq)
  }

  def makeRDD[T: ClassTag](seq: Seq[T]): RDD[T] = withScope {
    parallelize(seq)
  }

  def textFile(
      path: String): RDD[String] = withScope {
    new TextRDD(this, path)
  }

  def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] = {
    Array(func(rdd.iterator()))
  }
}

object SparkContext {
  private[spark] val SPARK_VERSION = "scala-learning.0.1"
  private val nextRddId = new AtomicInteger(0)

  /** Register a new RDD, returning its RDD ID */
  private[spark] def newRddId(): Int = nextRddId.getAndIncrement()

  def apply(master: String, appName: String): SparkContext = {
    new SparkContext(new SparkConf().setMaster(master).setAppName(appName))
  }
}