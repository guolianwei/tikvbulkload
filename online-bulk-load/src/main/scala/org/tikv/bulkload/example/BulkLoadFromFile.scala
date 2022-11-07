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
import org.apache.spark.sql.SparkSession
import org.apache.spark.tikvbulkload.{AarrayFileJdbcRDD, CkJdbcRDD}
import org.slf4j.LoggerFactory
import org.tikv.bulkload.RawKVBulkLoader

import java.sql.DriverManager;

object BulkLoadFromFile {

  private final val logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    var pdaddr: String = "10.17.39.100:2379"
    var prefix: String = ""
    var size: Long = 1000
    var partition: Int = 1200
    var exit: Boolean = true
    var exact: Boolean = false
    var dataPath:String="/root/bulkload/data/pdata_"
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

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val rdd = produceRdd(prefix,dataPath,spark);
    loader.bulkLoad(rdd)
    val end = System.currentTimeMillis()
    println(s"total time: ${(end - start) / 1000}s")
    println(s"total time: ${(end - start) / 1000}s")

    while (!exit) {
      Thread.sleep(1000)
    }
  }

  private def produceRdd(prefix:String,dataPath:String,spark: SparkSession) = {
    new AarrayFileJdbcRDD[(Array[Byte], Array[Byte])](spark.sparkContext, 1200,prefix,dataPath);
  }

  private def genKey(i: Long): String = f"$i%016d"
}
