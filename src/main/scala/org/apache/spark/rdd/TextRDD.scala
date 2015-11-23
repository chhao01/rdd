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

import java.io.{FileReader, BufferedReader, FileInputStream}

import org.apache.spark._
import org.apache.spark.util.NextIterator


class TextRDD(
    sc: SparkContext,
    filepath: String)
  extends RDD[String](sc) with Logging {

  override def compute(): Iterator[String] = {
    new NextIterator[String] {
      var reader = new BufferedReader(new FileReader(filepath))

      override def getNext(): String = {
        try {
          val value = reader.readLine()
          if (value == null) {
            finished = true
            null
          } else {
            reader.readLine()
          }
        } catch {
          case eof: Exception =>
            finished = true
            null
        }
      }

      override def close() {
        if (reader != null) {
          try {
            reader.close()
          } catch {
            case e: Exception =>
          } finally {
            reader = null
          }
        }
      }
    }
  }
}
