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

import com.basho.riak.client.api.RiakClient
import com.basho.riak.client.core.query.{Namespace, RiakObject, Location}
import com.basho.riak.spark.util.RiakObjectConversionUtil
import org.apache.spark.Logging

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag


abstract class AbstractRDDTest extends AbstractRiakSparkTest with Logging{

  protected def fetchAllFromBucket(ns:Namespace): List[(String,String)] = {
    val data =  ListBuffer[(String,String)]()
    withRiakDo(session=>
      foreachKeyInBucket(session, ns, (client, l: Location) =>{
        val v = readByLocation[String](client, l)
        data += ((l.getKeyAsString,v))
      })
    )
    data.toList
  }

  protected def readByLocation[T:ClassTag](riakSession: RiakClient, location: Location): T =
    readByLocation(riakSession, location, (l: Location, ro: RiakObject)  => {RiakObjectConversionUtil.from[T](l, ro)})
}
