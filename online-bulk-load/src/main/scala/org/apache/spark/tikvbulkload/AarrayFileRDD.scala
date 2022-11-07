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

package org.apache.spark.tikvbulkload

import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.util.NextIterator
import org.apache.spark.{Partition, SparkContext, TaskContext}

import java.io.{FileInputStream, ObjectInputStream}
import java.sql.{Connection, ResultSet}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import scala.reflect.ClassTag
class FilePartition(idx: Int) extends Partition {
  override def index: Int = idx
}

// TODO: Expose a jdbcRDD function in SparkContext and mark this as semi-private
/**
 * An RDD that executes a SQL query on a JDBC connection and reads results.
 * For usage example, see test case JdbcRDDSuite.
 *
 * @param getConnection a function that returns an open Connection.
 *   The RDD takes care of closing the connection.
 * @param sql the text of the query.
 *   The query must contain two ? placeholders for parameters used to partition the results.
 *   For example,
 *   {{{
 *   select title, author from books where ? <= id and id <= ?
 *   }}}
 * @param lowerBound the minimum value of the first placeholder
 * @param upperBound the maximum value of the second placeholder
 *   The lower and upper bounds are inclusive.
 * @param numPartitions the number of partitions.
 *   Given a lowerBound of 1, an upperBound of 20, and a numPartitions of 2,
 *   the query would be executed twice, once with (1, 10) and once with (11, 20)
 * @param mapRow a function from a ResultSet to a single row of the desired result type(s).
 *   This should only call getInt, getString, etc; the RDD takes care of calling next.
 *   The default maps a ResultSet to an array of Object.
 */
class AarrayFileJdbcRDD[T: ClassTag](
    sc: SparkContext,
    numPartitions: Int,
    prefix: String,
    dataPath:String,
    mapRow: (String, Iterator[(Array[Byte], Array[Byte])]) => T =
      AarrayFileJdbcRDD.FileToObjectArray _)
    extends RDD[T](sc, Nil)
    with Logging {

  override def getPartitions: Array[Partition] = {
    (0 until numPartitions).map { i => new FilePartition(i) }.toArray
  }

  override def compute(thePart: Partition, context: TaskContext): Iterator[T] =
    new NextIterator[T] {
      context.addTaskCompletionListener[Unit] { context => closeIfNeeded() }
      val part = thePart.asInstanceOf[FilePartition]
      val lower: Int = part.index + 1
      val oFileName = dataPath + lower;
      println("loading :" + oFileName)
      val in = new ObjectInputStream(new FileInputStream(oFileName))
      val saveNick = in.readObject().asInstanceOf[Array[(Array[Byte], Array[Byte])]]
      private val iterator1: Iterator[(Array[Byte], Array[Byte])] = saveNick.iterator
      override def getNext(): T = {
        if (iterator1.hasNext) {
          mapRow(prefix, iterator1)
        } else {
          finished = true
          null.asInstanceOf[T]
        }
      }
      override def close(): Unit = {
        in.close();
      }
    }
}

object AarrayFileJdbcRDD {
  def FileToObjectArray(
      prefix: String,
      rs: Iterator[(Array[Byte], Array[Byte])]): (Array[Byte], Array[Byte]) = {
    val tuple = rs.next()
    val newKey = prefix + new String(tuple._1)
    (newKey.getBytes, tuple._2)
  }
}
