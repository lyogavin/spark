package spark

import java.util.{HashMap => JHashMap}

import serializer.{SerializerInstance, DeserializationStream, SerializationStream}

import scala.collection.JavaConversions._
import scala.collection.mutable.LinkedList
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
      val partitionCount= 10 * 3

    val combinersArray = Array.fill(partitionCount){new JHashMap[K,LinkedList[Int]]()}
    val byteArrayStreams = Array.fill(partitionCount){new ByteArrayOutputStream()}
    val streams = Array.tabulate(partitionCount)(i => new spark.KryoSerializer().newInstance().serializeStream(new SnappyOutputStream(byteArrayStreams(i))))
    var data:Array[Object] = null
    var counts = Array.fill(partitionCount){-1}

    for ((k, v) <- iter) {
        var hashPar = k.hashCode % partitionCount
        if (hashPar < 0) {hashPar = hashPar + partitionCount}
    //    val beforesize = byteArrayStreams(hashPar).toByteArray()size()
        streams(hashPar).writeObject(v)
//        if (byteArrayStreams(hashPar).toByteArray().size() == beforesize)
  //      {
 //              logDebug("zero size found:"+k+","+v)
   //     }

      val oldC = combinersArray(hashPar).get(k)
      counts(hashPar) = counts(hashPar) + 1
      if (oldC == null) {
        combinersArray(hashPar).put(k, LinkedList(counts(hashPar)))
      } else {
        combinersArray(hashPar).put(k, oldC:+counts(hashPar))
      }
    }

    streams.foreach(_.close())

    //combiners.iterator
    new AbstractIterator[(K,C)] {
        private var data:java.util.LinkedList[Any] = null
        private var cur: Iterator[(K,LinkedList[Int])] = Iterator.empty
        private var curIdx: Int = -1
            def hasNext: Boolean =
            cur.hasNext || curIdx < partitionCount-1 && {
                curIdx = curIdx+1
               if(counts(curIdx) == -1){
               logDebug("empty partition, skip")
                   hasNext
               }else{
                data = null
                data = new java.util.LinkedList[Any]()
               logDebug("data raw size:" + byteArrayStreams(curIdx).toByteArray().length) 
                new spark.KryoSerializer().newInstance().deserializeStream(new SnappyInputStream(new ByteArrayInputStream(byteArrayStreams(curIdx).toByteArray()))).asIterator.foreach(data add _)
               logDebug("data size:" + data.length) 
               logDebug("counts:" + counts.mkString(",")) 
                cur = combinersArray(curIdx).toIterator; hasNext }}
        def next(): (K,C) = {if (hasNext) {
            val (curKey, curValue) = cur.next()
            combinersArray(curIdx).put(curKey, null)

            (curKey, {val c = createCombiner(data(curValue(0)).asInstanceOf[V]);curValue.slice(1, curValue.length).foreach((i:Int) => {mergeValue(c, data(i).asInstanceOf[V]);data.set(i, null)});c})
            }else Iterator.empty.next()}
}
  }

  def combineValuesByKey_old(iter: Iterator[(K, V)]) : Iterator[(K, C)] = {
    val combiners = new JHashMap[K, C]
    var vcount: Long = 0
    var ksize: Long = 0
    var vsize: Long = 0
    for ((k, v) <- iter) {
      val oldC = combiners.get(k)
      if (oldC == null) {
        vcount += 1l
        ksize = ksize + {k match {
            case str: String => str.length
            case other => 1l}}
        vsize = vsize+ {v match {
            case str: String => str.length
            case a: Array[Byte] => a.length
            case other => 1l}}
        if (vcount % 15 == 0) {
            logDebug("size of hash(kcount,vcount,ksize,vsize): " + System.identityHashCode(combiners)  + combiners.size + "," + vcount + ","+ksize + ","+vsize)
        }
        combiners.put(k, createCombiner(v))
      } else {
        vcount += 1l
        vsize = vsize + {v match {
            case str: String => str.length
            case a: Array[Byte] => a.length
            case other => 1l}}
        if (vcount % 15 == 0) {
            logDebug("size of hash(kcount,vcount,ksize,vsize): " + System.identityHashCode(combiners) + combiners.size + "," + vcount + ","+ksize + ","+vsize)
        }
        combiners.put(k, mergeValue(oldC, v))
      }
    }
            logDebug("Construction hash done,(kcount,vcount,ksize,vsize): " + System.identityHashCode(combiners) + combiners.size + "," + vcount + ","+ksize + ","+vsize)
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
}

private[spark] abstract class AbstractIterator[+A] extends Iterator[A]
