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

import com.basho.riak.spark.util.RiakObjectConversionUtil
import org.apache.spark.TaskContext
import com.basho.riak.client.core.query.{Location, RiakObject}
import org.junit.{Ignore, Test, Assert}
import com.basho.riak.spark._
import org.mockito.Mockito

case class TSData(latitude: Float, longitude: Float, timestamp: String, user_id: String, gauge1: Int, gauge2: String)

class RDDTest extends AbstractRDDTest{
  private val CREATION_INDEX = "creationNo"

  protected override def jsonData(): String =
    "[" +
        "  { key: 'my_key_1', indexes: {creationNo: 1}, value: {latitude: 28.946907, longitude: -82.00319, timestamp: '2014-11-24T13:14:04.823Z', user_id: '2c1421c4-161d-456b-a1c1-63ceededc3d5'," +
              "gauge1: 142}}" +
        ", {key: 'my_key_2', indexes: {creationNo: 2}, value: {latitude: 28.946907, longitude: -82.00319, timestamp: '2014-11-24T13:14:05.823Z', user_id: '2c1421c4-161d-456b-a1c1-63ceededc3d5'," +
              "gauge1: 145, gauge2: 'min'}}" +
        ", {indexes: {creationNo: 3}, value: {latitude: 28.946907, longitude: -82.00319, timestamp: '2014-11-24T13:14:05.823Z', user_id: '2c1421c4-161d-456b-a1c1-63ceededc3d5'," +
              "gauge1: 0}}" +
        ", {indexes: {creationNo: 4}, value: {latitude: 28.946907, longitude: -82.00319, timestamp: '2014-11-24T13:14:05.823Z', user_id: '2c1421c4-161d-456b-a1c1-63ceededc3d5'," +
              "gauge1: 400, gauge2: '128'}}" +
        ", {indexes: {creationNo: 5}, value: {latitude: 28.946907, longitude: -82.00319, timestamp: '2014-11-24T13:14:04.823Z', user_id: 'e36fb24a-5f61-4107-a5a2-405647a3a6bd'}}" +
        ", {indexes: {creationNo: 6}, value: {latitude: 28.946907, longitude: -82.00319, timestamp: '2014-11-24T13:14:05.823Z', user_id: 'e36fb24a-5f61-4107-a5a2-405647a3a6bd'}}" +
        ", {indexes: {creationNo: 7}, value: {latitude: 28.946907, longitude: -82.00319, timestamp: '2014-11-24T13:14:06.823Z', user_id: 'e36fb24a-5f61-4107-a5a2-405647a3a6bd'}}" +
        ", {indexes: {creationNo: 8}, value: {latitude: 28.946907, longitude: -82.00319, timestamp: '2014-11-24T13:14:07.823Z', user_id: 'e36fb24a-5f61-4107-a5a2-405647a3a6bd'}}" +
        ", {indexes: {creationNo: 9}, value: {latitude: 28.946907, longitude: -82.00319, timestamp: '2014-11-24T13:14:06.823Z', user_id: '36e3616f-59f2-4096-b5cb-3ab94db5da41'}}" +
        ", {indexes: {creationNo: 10}, value: {latitude: 28.946907, longitude: -82.00319, timestamp: '2014-11-24T13:14:07.823Z', user_id: '36e3616f-59f2-4096-b5cb-3ab94db5da41'}}" +
    "]"

  private def computeAndGatherAllResults[T](rdd: RiakRDD[T]): List[T] = {
    val partitions = rdd.getPartitions
    val tc = Mockito.mock(classOf[TaskContext])
    rdd.compute(partitions.iterator.next(), tc).toList
  }

  @Test
  def first(){
    val rdd = sc.riakBucket(DEFAULT_NAMESPACE)
      .query2iRange(CREATION_INDEX, 1, 10)

  }

  @Ignore("Ignored until the fix for '2i inconsistency reads' will be released")
  @Test
  def check2IPageableProcessing() = {
    val rdd = sc.riakBucket[Map[String,_]](DEFAULT_NAMESPACE)
      .query2iRange(CREATION_INDEX, 1, 10)

    val results = computeAndGatherAllResults(rdd.asInstanceOf[RiakRDD[String]])
    Assert.assertEquals(10, results.size)
  }

  @Test
  def check2IRangeQuery() = {
    val rdd = sc.riakBucket[Map[String, _]](DEFAULT_NAMESPACE)
                .query2iRange(CREATION_INDEX, 1, 2)

    val results = computeAndGatherAllResults(rdd.asInstanceOf[RiakRDD[String]])
    assertEqualsUsingJSONIgnoreOrder("[" +
        "{latitude: 28.946907, longitude: -82.00319, timestamp: '2014-11-24T13:14:04.823Z', user_id: '2c1421c4-161d-456b-a1c1-63ceededc3d5'," +
          "gauge1: 142" +
          "}," +
        "{latitude: 28.946907, longitude: -82.00319, timestamp: '2014-11-24T13:14:05.823Z', user_id: '2c1421c4-161d-456b-a1c1-63ceededc3d5'," +
          "gauge1: 145, gauge2: 'min'" +
          "}" +
        "]",
      results)
  }

  @Test
  def checkUDTMapping(): Unit ={
    val rdd = sc.riakBucket[TSData](DEFAULT_NAMESPACE)
      .query2iRange(CREATION_INDEX, 1, 2)

    val results = computeAndGatherAllResults(rdd)
    logInfo(results.toString())
  }

  @Test
  def check2IRangeQueryPairRDD() = {
    val rdd = sc.riakBucket[String, Map[String, _]](DEFAULT_NAMESPACE, (k: Location, r: RiakObject) => (k.getKeyAsString, RiakObjectConversionUtil.from[Map[String, _]](k,r)))
      .query2iRange(CREATION_INDEX, 1, 2)
    val results = computeAndGatherAllResults(rdd.asInstanceOf[RiakRDD[(String, Map[String, _])]])
    assertEqualsUsingJSONIgnoreOrder("[" +
      "['my_key_1',{latitude: 28.946907, longitude: -82.00319, timestamp: '2014-11-24T13:14:04.823Z', user_id: '2c1421c4-161d-456b-a1c1-63ceededc3d5'," +
      "gauge1: 142" +
      "}]," +
      "['my_key_2',{latitude: 28.946907, longitude: -82.00319, timestamp: '2014-11-24T13:14:05.823Z', user_id: '2c1421c4-161d-456b-a1c1-63ceededc3d5'," +
      "gauge1: 145, gauge2: 'min'" +
      "}]" +
      "]",
      results)
  }
}