/*
 * Copyright 2021 TiKV Project Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tikv.bulkload.example

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import org.tikv.bulkload.RawKVBulkLoader

import scala.util.Random
import org.apache.spark.tikvbulkload.CkJdbcRDD

import java.sql.DriverManager;
object BulkLoadJdbc {

  private final val logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    var pdaddr: String = "10.17.39.100:2379"
    var prefix: String = ""
    var size: Long = 1000
    var partition: Int = 1200
    var exit: Boolean = true
    var exact: Boolean = false
    logger.info(s"""
         |*****************
         |pdaddr=$pdaddr
         |prefix=$prefix
         |size=$size
         |partition=$partition
         |exit=$exit
         |exact=$exact
         |*****************
         |""".stripMargin)

    if (size / partition > Int.MaxValue) {
      throw new Exception("size / partition > Int.MaxValue")
    }

    val start = System.currentTimeMillis()

    val sparkConf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.app.name", getClass.getName)

    val loader = new RawKVBulkLoader(pdaddr)

    for (i <- 1 to 1200) {
      val spark = SparkSession.builder.config(sparkConf).getOrCreate()
      val sql =
        "select * from nucleic_information_copy_all where date_of_birth=? order by id_number asc"
      val rdd: _root_.org.apache.spark.rdd.RDD[(Array[Byte], Array[Byte])] =
        produceRdd(spark, sql, i)
      loader.bulkLoad(rdd)
    }

    val end = System.currentTimeMillis()

    logger.info(s"total time: ${(end - start) / 1000}s")

    while (!exit) {
      Thread.sleep(1000)
    }
  }

  private def produceRdd(spark: SparkSession, sql: String, mIndex: Int) = {
    val jdbcRdd = new CkJdbcRDD(spark.sparkContext, () => {
      Class.forName("ru.yandex.clickhouse.ClickHouseDriver") // 驱动包

      val url = "jdbc:clickhouse://10.17.39.100:18123/merit?socket_timeout=600000" // url路径
      val user = "default" // 账号
      val password = "123456" // 密码
      DriverManager.getConnection(url, user, password)
    }, sql, mIndex, mIndex, 1)
      .map(f => {
        val key = f(0)
        val mon = key.toString.substring(6, 14)
        val str = f.zipWithIndex.reduce((x, y) => {
          if (x._2 == 0) {
            val vRes =
              (x._2 + 1).toString + "='" + x._1.toString + "'," + (y._2 + 1).toString + "='" + y._1.toString + "'"
            (vRes, 2)
          } else {
            val vRes = x._1.toString + "," + (y._2 + 1).toString + "='" + y._1.toString + "'"
            (vRes, 2)
          }
        })
        //        println(str._1)
        (mon + key, "{" + str._1 + "}")
      })
      .map(kv => {
        (kv._1.getBytes, kv._2.getBytes)
      })

    val tuples = jdbcRdd.collect()
    println("data len "+mIndex+" ->" + tuples.length)
    val rdd = spark.sparkContext.parallelize(tuples)
    rdd
  }

  private def genKey(i: Long): String = f"$i%016d"
}
