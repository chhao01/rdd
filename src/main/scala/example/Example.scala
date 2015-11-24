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
