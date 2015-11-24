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

import org.scalatest.FunSuite

import org.apache.spark.SharedSparkContext

case class User(id: Long, name: String, age: Int)
case class Purchase(tid: Long, userId: Long, item: String, price: Float)

class RDDSuite extends FunSuite with SharedSparkContext {
  test("basic operations") {
    val nums = sc.makeRDD(Array(1, 2, 3, 4))
    assert(nums.collect().toList === List(1, 2, 3, 4))
    assert(nums.map(_.toString).collect().toList === List("1", "2", "3", "4"))
    assert(nums.filter(_ > 2).collect().toList === List(3, 4))
    assert(nums.flatMap(x => 1 to x).collect().toList === List(1, 1, 2, 1, 2, 3, 1, 2, 3, 4))
    assert(nums.union(nums).collect().toList === List(1, 2, 3, 4, 1, 2, 3, 4))
    assert(nums.collect({ case i if i >= 3 => i.toString }).collect().toList === List("3", "4"))
    assert(nums.keyBy(_.toString).collect().toList === List(("1", 1), ("2", 2), ("3", 3), ("4", 4)))

    intercept[UnsupportedOperationException] {
      nums.filter(_ > 5).reduce(_ + _)
    }
  }

  test("take") {
    val nums = sc.makeRDD(Range(1, 1000))
    assert(nums.take(0).size === 0)
    assert(nums.take(1) === Array(1))
    assert(nums.take(3) === Array(1, 2, 3))
  }

  test("pair function") {
    import RDD._
    val users = sc.makeRDD(1 to 10).map(i => User(i, s"Name $i", (i + 103) % 23))
    val purchases = sc.makeRDD(
      Purchase(1, 1, "Item1", 12.31f) ::
      Purchase(2, 1, "Item2", 32.1f) ::
      Purchase(3, 2, "Item3", 888.8f) :: Nil)

    val userPair = users.map(u => (u.id, u))
    val purchasePair = purchases.map(p => (p.userId, p))

    userPair.join(purchasePair).mapPartitions { it =>
      it.map { case (uid, (user, purchase)) => (user.age, purchase.price) }
    }.groupBy(_._1).map { case (age, it) => (age, it.map(_._2).sum) }.foreach { case (age, total) =>
      println(s"Age:$age => Comsume: $total")
    }
  }
}
