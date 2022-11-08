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
import org.apache.spark.tikvbulkload.AarrayFileJdbcRDD
import org.slf4j.LoggerFactory
import org.tikv.bulkload.RawKVBulkLoader
import org.tikv.common.{TiConfiguration, TiSession}
import org.tikv.kvproto.Kvrpcpb
import org.tikv.raw.RawKVClient
import org.tikv.shade.com.google.protobuf.ByteString

import java.io.{FileInputStream, ObjectInputStream}
import java.nio.charset.Charset
import java.util
import java.util.Optional;
import scala.collection.mutable.ListBuffer

object BulkLoadFromFileAndEqualTest {

  private final val logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    var pdaddr: String = "10.17.39.100:2379"
    var prefix: String = "T1_"
    var size: Long = 1000
    var partition: Int = 1200
    var exit: Boolean = true
    var exact: Boolean = false
    var months = args(0).toInt
    prefix = args(1)
    var batchQ = args(2).toInt
    var dataPath = args(3)
    var onlyGet = args(4).toInt
    println(s"""
         |*****************
         |pdaddr=$pdaddr
         |prefix=$prefix
         |size=$size
         |partition=$partition
         |exit=$exit
         |exact=$exact
         |months=$months
         |batchQ=$batchQ
         |onlyGet=$onlyGet
         |*****************
         |""".stripMargin)

    if (size / partition > Int.MaxValue) {
      throw new Exception("size / partition > Int.MaxValue")
    }

    val start = System.currentTimeMillis()

    val sparkConf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.app.name", getClass.getName)
      .setIfMissing("spark.driver.maxResultSize", "2048m")

    val loader = new RawKVBulkLoader(pdaddr)
    if (onlyGet != 1) {
      val spark = SparkSession.builder.config(sparkConf).getOrCreate()
      val rdd = produceRdd(months, dataPath, prefix, spark);
      loader.bulkLoad(rdd)
    }

    val end = System.currentTimeMillis()
    println(s"total time: ${(end - start) / 1000}s")
    println(s"begin to check value")
    val checkBegin = System.currentTimeMillis();
    val conf = TiConfiguration.createRawDefault("10.17.39.100:2379")
    conf.setRawKVBatchWriteTimeoutInMS(10000)
    conf.setDBPrefix("nucleic_information")
    val session = TiSession.create(conf)
    val client = session.createRawClient
    var seqSum = new ListBuffer[(String, Int)];
    for (i <- 1 to months) {
      val tuple = checkKey(prefix, dataPath, batchQ, i, client)
      seqSum ++= tuple
    }
    seqSum
      .groupBy(_._1)
      .map(f => {
        val i = f._2.reduce((x, y) => {
          (f._1, x._2 + y._2)
        })
        (f._1, i._2)
      })
      .foreach(f => println(f._1 + " -> " + f._2))
    println(s"check value end")
    val checkEnd = System.currentTimeMillis()
    println(s"check value end ${(checkEnd - checkBegin) / 1000} s")
    while (!exit) {
      Thread.sleep(1000)
    }
  }

  private def checkKey(
      prefix: String,
      dataPath: String,
      batchQ: Int,
      i: Int,
      client: RawKVClient): List[(String, Int)] = {
    val oFileName = dataPath + i;
    println("checking " + oFileName)
    val in = new ObjectInputStream(new FileInputStream(oFileName))
    val saveNick = in.readObject().asInstanceOf[Array[(Array[Byte], Array[Byte])]]

    var resList = new ListBuffer[(String, Int)]
    val stringToInt1 = saveNick.zipWithIndex
      .map(f => {
        (f._2 % batchQ, f._1)
      })
      .groupBy(_._1)
      .toList
      .flatMap(x => {
        val kvs = x._2
        val batchList: util.List[ByteString] =
          new util.ArrayList[ByteString](kvs.length);
        val valuesKeV = kvs
          .map(f => {
            val strKey = new String(f._2._1)
            val keyFinal = prefix + strKey;
            batchList.add(ByteString.copyFromUtf8(keyFinal))
            (keyFinal, new String(f._2._2))
          })
          .toMap
        val pairs = client.batchGet(batchList)
        val resSize = pairs.size()
        if (resSize > 0) {
          println(s"get [$resSize] values from tikv by keys")
          resList = getTestedValues(pairs, valuesKeV)
          val size = resList.size
          println(s"get [$size] cmp res from tikv by keys")
        } else {
          println("can't get value from tikv by key" + pairs)
        }
        val stringToInt = resList
          .groupBy(_._1)
          .map(x => {
            val tuple = x._2.reduce((y1, y2) => {
              (x._1, y1._2 + y2._2)
            })
            (x._1, tuple._2)
          })
        stringToInt.toList
      })
    val stringToInt = stringToInt1
      .groupBy(_._1)
      .map(x => {
        val i = x._2.reduce((y1, y2) => (x._1, y1._2 + y2._2))
        (x._1, i._2)
      })
      .toList
    stringToInt
  }

  private def getTestedValues(
      pairs: util.List[Kvrpcpb.KvPair],
      valuesKeV: Map[String, String]): ListBuffer[(String, Int)] = {

    val listBuffer = new ListBuffer[(String, Int)]
    pairs.forEach(pair => {
      val key = pair.getKey
      val value = pair.getValue
      val res = if (value != null) {
        val valueInTiKvs = value.toStringUtf8
        val valueInFile = valuesKeV.get(key.toStringUtf8)
        if (valueInTiKvs.equals(valueInFile.get)) {
          ("eq", 1)
        } else {
          println(
            s"not equals value in tikv by key $key -> value in file[ $valueInFile] !== in tikv[$valueInTiKvs] ")
          ("ne", 1)
        }
      } else {
        println(s"not found value in tikv by key $key")
        ("nf", 1)
      }
      listBuffer += res
    })
    listBuffer
  }

  private def produceRdd(months: Int, dataPath: String, prefix: String, spark: SparkSession) = {
    new AarrayFileJdbcRDD[(Array[Byte], Array[Byte])](spark.sparkContext, months, prefix, dataPath);
  }

  private def genKey(i: Long): String = f"$i%016d"
}
