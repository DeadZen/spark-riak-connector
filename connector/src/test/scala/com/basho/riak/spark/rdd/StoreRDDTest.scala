/**
 * Copyright (c) 2015 Basho Technologies, Inc.
 *
 * This file is provided to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.basho.riak.spark.rdd

import java.util.concurrent.atomic.AtomicLong

import com.basho.riak.client.api.RiakClient
import com.basho.riak.client.api.annotations.{RiakIndex, RiakKey}
import com.basho.riak.client.core.query.{Namespace, Location, RiakObject}
import com.basho.riak.client.core.query.indexes.LongIntIndex
import com.basho.riak.spark.util.RiakObjectConversionUtil
import com.basho.riak.spark.writer.{WriteDataMapper, WriteDataMapperFactory}
import org.apache.spark.rdd.RDD
import org.junit.Test

import scala.annotation.meta.field

import com.basho.riak.spark._

import org.junit.Assert._

/**
 * Domain Object for ORM checks
 */
case class ORMDomainObject(
    @(RiakKey@field)
    user_id: String,

    @(RiakIndex@field) (name = "groupId")
    group_id: Long,

    login: String)

class RDDStoreTest  extends AbstractRDDTest {
  @Test
  def storePairRDDUsingCustomMapper(): Unit = {
    val perUserTotals = sc.parallelize(List("u2"->1, "u3"->2, "u1"->3))

    /**
     * Custom value writer factory which uses totals as a key.
     */
    implicit val vwf = new WriteDataMapperFactory[(String,Int)]{
      override def dataMapper(bucket: BucketDef): WriteDataMapper[(String, Int)] = {
        new WriteDataMapper[(String, Int)] {
          override def mapValue(value: (String, Int)): (String, Any) = {
            (value._2.toString, RiakObjectConversionUtil.to(value._1))
          }
        }
      }
    }

    perUserTotals.saveAsRiakBucket(BucketDef(DEFAULT_NAMESPACE_4STORE))

    val data = fetchAllFromBucket(DEFAULT_NAMESPACE_4STORE)

    // Since Riak may returns results in any order, we need to ignore order at all
    assertEqualsUsingJSONIgnoreOrder(
      "[" +
        "['2','u3']," +
        "['3','u1']," +
        "['1','u2']" +
        "]", data)
  }

  @Test
  def storeUsingORMAbilities(): Unit = {
    val data = List(ORMDomainObject("u1", 100, "user 1"), ORMDomainObject("u2", 200,"user 2"), ORMDomainObject("u3", 100, "user 3"))
    val rdd:RDD[ORMDomainObject] = sc.parallelize(data, 1)

    rdd.saveAsRiakBucket(BucketDef(DEFAULT_NAMESPACE_4STORE))

    // Let's read values by bucket keys
    val dataByKeys = sc.riakBucket[ORMDomainObject](DEFAULT_NAMESPACE_4STORE)
      .queryBucketKeys("u1", "u2")
      .collect()

    // TODO: Remove  ${json-unit.ignore} as soon as read conversion logic will be re-implemented to use java client Converter.toDomain method,
    assertEqualsUsingJSONIgnoreOrder("[" +
      "{login:'user 1', group_id:'${json-unit.ignore}', user_id:'${json-unit.ignore}'}," +
      "{login:'user 2', group_id:'${json-unit.ignore}', user_id:'${json-unit.ignore}'}" +
      "]", dataByKeys)

    val dataBy2iRange = sc.riakBucket[ORMDomainObject](DEFAULT_NAMESPACE_4STORE)
      .query2iRange("groupId", 100L, 200L)
      .collect()

    assertEqualsUsingJSONIgnoreOrder("[" +
      "{login:'user 1', group_id:'${json-unit.ignore}', user_id:'${json-unit.ignore}'}," +
      "{login:'user 2', group_id:'${json-unit.ignore}', user_id:'${json-unit.ignore}'}," +
      "{login:'user 3', group_id:'${json-unit.ignore}', user_id:'${json-unit.ignore}'}" +
      "]", dataBy2iRange)
  }

  @Test
  def storeRDDWith2iUsingCustomMapper(): Unit = {
    val size = 4
    val rdd = sc.parallelize(1 to size, 1)

    /**
     * ValueWriterFactory responsible for populating each stored object with the proper CREATION_INDEX value
     */
    implicit val vwf = new WriteDataMapperFactory[Int] {
      override def dataMapper(bucket: BucketDef): WriteDataMapper[Int] = {
        new WriteDataMapper[Int] {
          /**
           * Save operation performed on each partition, therefore, for production usage
           * the following Atomic should be replaced by a distributed counter
           */
          val counter = new AtomicLong()

          override def mapValue(value: Int): (String, RiakObject) = {
            val ro = RiakObjectConversionUtil.to(value)

            ro.getIndexes.getIndex[LongIntIndex, LongIntIndex.Name](LongIntIndex.named("creationNo"))
              .add(counter.getAndIncrement)

            (null, ro) // Key is null, it will be  generated by Riak
          }
        }
      }
    }

    rdd.saveAsRiakBucket(BucketDef(DEFAULT_NAMESPACE_4STORE))

    val data = sc.riakBucket[Int](DEFAULT_NAMESPACE_4STORE)
      .query2iRange("creationNo", 0L, size - 1)
      .collect()

    assertEqualsUsingJSONIgnoreOrder(
      "[" +
        "1," +
        "2," +
        "3," +
        "4" +
        "]", data)
  }
  @Test
  def storeTuple1() = {
    sc.parallelize(List(Tuple1("key1"), Tuple1("key2"), Tuple1("key3")), 1)
      .saveToRiak(DEFAULT_NAMESPACE_4STORE)

    val t1Data = fetchAllFromBucket(DEFAULT_NAMESPACE_4STORE)

    // Keys should be generated on the Riak side, therefore they will be ignored
    assertEqualsUsingJSONIgnoreOrder("[" +
      "['${json-unit.ignore}', 'key1']," +
      "['${json-unit.ignore}', 'key2']," +
      "['${json-unit.ignore}', 'key3']" +
      "]", t1Data)

    verifyContentTypeEntireTheBucket("text/plain", DEFAULT_NAMESPACE_4STORE)
  }

  @Test
  def storeTuple2(): Unit = {
    sc.parallelize(List("key1"->1, "key2"->2, "key3"->3), 1)
      .saveToRiak(DEFAULT_NAMESPACE_4STORE)

    // TODO: FIX proper content type handling - it must be 'text/plain'
    verifyContentTypeEntireTheBucket("application/json")

    val data = sc.riakBucket[Int](DEFAULT_NAMESPACE_4STORE)
      .queryBucketKeys("key1", "key2", "key3")
      .collect()

    assertEquals(3, data.length)
    assertEqualsUsingJSONIgnoreOrder("[1,2,3]", data)
  }

  @Test
  def storeTuple3(): Unit = {
    sc.parallelize(List(("key1",1,11), ("key2",2,22), ("key3",3,33)), 1)
      .saveToRiak(DEFAULT_NAMESPACE_4STORE)

    verifyContentTypeEntireTheBucket("application/json")

    val data = sc.riakBucket[List[Int]](DEFAULT_NAMESPACE_4STORE)
      .queryBucketKeys("key1", "key2", "key3")
      .collect()

    assertEqualsUsingJSONIgnoreOrder("[" +
        "[1,11]," +
        "[2,22]," +
        "[3,33]" +
      "]", data)
  }

  @Test
  def list_And_OtherProductDescendants_ShouldBe_StoredWithDefaultMapper() = {
    sc.parallelize(List(List("key1",1), List("key2",2), List("key3",3)), 1)
      .saveToRiak(DEFAULT_NAMESPACE_4STORE)

    val lData = fetchAllFromBucket(DEFAULT_NAMESPACE_4STORE)

    // Keys should be generated on the Riak side, therefore they will be ignored
    assertEqualsUsingJSONIgnoreOrder("[" +
        "['${json-unit.ignore}', '[\"key1\",1]']," +
        "['${json-unit.ignore}', '[\"key2\",2]']," +
        "['${json-unit.ignore}', '[\"key3\",3]']" +
      "]", lData)

    verifyContentTypeEntireTheBucket("application/json", DEFAULT_NAMESPACE_4STORE)
  }

  private def verifyContentTypeEntireTheBucket(expected: String, ns: Namespace = DEFAULT_NAMESPACE_4STORE): Unit = {
    withRiakDo(session =>{
      foreachKeyInBucket(session, ns, (client:RiakClient, l:Location) => {
        val ro = readByLocation[RiakObject](session, l)
        assertEquals(s"Unexpected RiakObject.contentType\nExpected\t:$expected\nActual\t:${ro.getContentType}", expected, ro.getContentType)
      })
    })
  }
}