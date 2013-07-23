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

package spark

import java.util.{HashMap => JHashMap}

import scala.collection.JavaConversions._

import serializer.{SerializerInstance, DeserializationStream, SerializationStream}

import scala.collection.mutable.{LinkedList, ArrayBuffer}
import spark.Logging
import org.xerial.snappy.{SnappyInputStream, SnappyOutputStream}
import org.xerial.snappy.Snappy
import java.io.ByteArrayOutputStream
import java.io.ByteArrayInputStream

/** A set of functions used to aggregate data.
  * 
  * @param createCombiner function to create the initial value of the aggregation.
  * @param mergeValue function to merge a new value into the aggregation result.
  * @param mergeCombiners function to merge outputs from multiple mergeValue function.
  */
case class Aggregator[K, V, C] (
    val createCombiner: V => C,
    val mergeValue: (C, V) => C,
    val mergeCombiners: (C, C) => C)
  extends Logging {

  def combineValuesByKey(iter: Iterator[(K, V)]) : Iterator[(K, C)] = {
    val combiners = new JHashMap[K, C]
    for ((k, v) <- iter) {
      val oldC = combiners.get(k)
      if (oldC == null) {
        combiners.put(k, createCombiner(v))
      } else {
        combiners.put(k, mergeValue(oldC, v))
      }
    }
    combiners.iterator
  }

  def combineCombinersByKey(iter: Iterator[(K, C)]) : Iterator[(K, C)] = {
    val combiners = new JHashMap[K, C]
    for ((k, c) <- iter) {
      val oldC = combiners.get(k)
      if (oldC == null) {
        combiners.put(k, c)
      } else {
        combiners.put(k, mergeCombiners(oldC, c))
      }
    }
    combiners.iterator
  }

  abstract class CompressionStreamsIterator[+A] extends Iterator[A]

  def combineValuesByKeyInCompression(iter: Iterator[(K, V)]) : Iterator[(K, C)] = {
    val partitionCount = System.getProperty("spark.reduce.side.combine.compression.partition", "11").toInt
    logDebug("combine compression enabled, partition count:"+partitionCount)
    val indexArray = Array.fill(partitionCount){new JHashMap[K,LinkedList[Int]]()}
    val byteArrayStreams = Array.fill(partitionCount){new ByteArrayOutputStream()}
    val serializeStreams = Array.tabulate(partitionCount)(i => SparkEnv.get.serializer.
      newInstance().serializeStream(new SnappyOutputStream(byteArrayStreams(i))))
    var data: Array[Object] = null
    var counts = Array.fill(partitionCount){-1}

    for ((k, v) <- iter) {
      var keyHash = k.hashCode % partitionCount
      if (keyHash < 0) {
        keyHash = keyHash + partitionCount
      }
      serializeStreams(keyHash).writeObject(v)

      val list = indexArray(keyHash).get(k)
      counts(keyHash) = counts(keyHash) + 1

      if (list == null) {
        indexArray(keyHash).put(k, LinkedList(counts(keyHash)))
      } else {
        list.append(LinkedList(counts(keyHash)))
      }
    }

    serializeStreams.foreach(_.close())

    for (i <- 0 to partitionCount - 1)
    {
      logDebug("Compression partition [" + i + "], size:" + 
        byteArrayStreams(i).size() + ", objects count:" + 
        counts(i))
    }

    new CompressionStreamsIterator[(K,C)] {
      private var uncompressedData: ArrayBuffer[Any] = null
      private var cur: Iterator[(K,LinkedList[Int])] = Iterator.empty
      private var curIdx: Int = -1
      def hasNext: Boolean =
        cur.hasNext || curIdx < partitionCount - 1 && {
          curIdx = curIdx + 1
          if (counts(curIdx) == -1) {
            hasNext
          } else {
            uncompressedData = new ArrayBuffer[Any](counts(curIdx) + 1)
            SparkEnv.get.serializer.newInstance().deserializeStream(new SnappyInputStream(
              new ByteArrayInputStream(byteArrayStreams(curIdx).toByteArray()))).asIterator.
              foreach(uncompressedData += _)
            cur = indexArray(curIdx).toIterator
            hasNext 
          }
        }
      def next(): (K,C) = 
        if (hasNext) {
          val (curKey, curValue) = cur.next()
          indexArray(curIdx).put(curKey, null)

          val combiner = createCombiner(uncompressedData(curValue(0)).asInstanceOf[V])
          curValue.slice(1, curValue.length).foreach(
            (i: Int) => {
              mergeValue(combiner, uncompressedData(i).asInstanceOf[V])
              uncompressedData(i) = null
            })
          (curKey, combiner)
        } else {
          Iterator.empty.next()
        }
    }
  }
}

