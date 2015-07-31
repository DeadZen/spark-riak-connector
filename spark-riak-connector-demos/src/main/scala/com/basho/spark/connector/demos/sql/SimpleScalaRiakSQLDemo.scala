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
package com.basho.spark.connector.demos

import com.basho.riak.client.core.query.Namespace
import com.basho.spark.connector.rdd.{RiakFunctions, RiakObjectData, BucketDef}
import org.apache.spark.{SparkContext, SparkConf}
import com.basho.spark.connector._
/**
 * Really simple demo program which calculates the number of records loaded
 * from the Riak using 2i range query
 */
object SimpleScalaRiakSQLDemo {
  private val SOURCE_DATA = new Namespace("users")

  case class UserData(user_id: String, name: String, age: Int, category: String)
  private val TEST_DATA: String =
    "[" +
      "  {key: 'key1', indexes: {creationNo: 1}, value: {user_id: 'u1', name: 'Ben', age: 23, category: 'Developer'}}" +
      ", {key: 'key2', indexes: {creationNo: 2}, value: {user_id: 'u2', name: 'Clair', age: 16, category: 'Student'}}" +
      ", {key: 'key3', indexes: {creationNo: 3}, value: {user_id: 'u3', name: 'John', age: 21}}" +
      ", {key: 'key4', indexes: {creationNo: 4}, value: {user_id: 'u3', name: 'Chris', age: 50, category: 'Pensioner'}}" +
      ", {key: 'key5', indexes: {creationNo: 5}, value: {user_id: 'u5', name: 'Mary', age: 14, category: 'Student'}}" +
      ", {key: 'key6', indexes: {creationNo: 6}, value: {user_id: 'u6', name: 'George', age: 31, category: 'Unemployed'}}" +
    "]"


  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Simple Scala Riak Demo")

    setSparkOpt(sparkConf, "spark.master", "local")
    setSparkOpt(sparkConf, "spark.riak.connection.host", "192.168.161.107:51087,192.168.161.107:52087,192.168.161.107:53087")
    //setSparkOpt(sparkConf, "spark.riak.connection.host", "127.0.0.1:10017")

    createTestData(sparkConf)
    val sc = new SparkContext(sparkConf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // to enable toDF()
    import sqlContext.implicits._

    // User defined function
    sqlContext.udf.register("strLen", (s: String) => s.length)

    // read from Riak with UDT to enable schema inference using reflection
    val df = sc.riakBucket[UserData](SOURCE_DATA).query2iRange("creationNo", 1, 6).toDF
    df.registerTempTable("users")
    
    println("count by category")
    df.groupBy("category").count().show()
    
    println("sort by num of letters")
    sqlContext.sql("select user_id, name, strLen(name) nameLen from users order by nameLen").show
    
    println("filter age >= 21")
    sqlContext.sql("select * from users where age >= 21").show
  }

  private def createTestData(sparkConf: SparkConf): Unit = {
    val rf = RiakFunctions(sparkConf)

    rf.withRiakDo(session => {
      rf.createValues(session, SOURCE_DATA, TEST_DATA, true )
    })
  }

  private def setSparkOpt(sparkConf: SparkConf, option: String, defaultOptVal: String): SparkConf = {
    val optval = sparkConf.getOption(option).getOrElse(defaultOptVal)
    sparkConf.set(option, optval)
  }
}
