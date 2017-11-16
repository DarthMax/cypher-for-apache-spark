/*
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.caps.demo

import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SparkSession, functions}
import org.opencypher.caps.demo.Configuration.MasterAddress
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.config.Config
import org.opencypher.caps.api.record._
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.spark.{CAPSRecords, CAPSSession}

object CosmosDBDemo extends App{

  val conf = new SparkConf(true)
  conf.set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
  conf.set("spark.kryo.registrator", classOf[CypherKryoRegistrar].getCanonicalName)

  implicit lazy val session = SparkSession.builder()
    .config(conf)
    .master(MasterAddress.get())
    .appName(s"cypher-for-apache-spark-benchmark-${Calendar.getInstance().getTime}")
    .getOrCreate()

  import session.implicits._

  implicit val capsSession = CAPSSession.builder(session).build

  val readConfig = Config(Map(
    "Endpoint" -> "https://neo-graph-spark-test.documents.azure.com:443/",
    "Masterkey" -> "cXKnPlis5ljJymOUSigvbaNRAJRosxcaSsJWrLaOFJk0brq19HJduAg0KcYtzfBPVQUbO6O0xv09Pa7DccdFfw==",
    "Database" -> "test-graph",
    "Collection" -> "test-graph-id",
    "SamplingRatio" -> "1.0"))

  val labels = session.sqlContext.read.cosmosDB(readConfig).select($"_isEdge", $"label").distinct()

  val (edgeLabelRows, vertexLabelRows) =  labels.collect.partition { row =>
    row.get(0) match {
      case true => true
      case _    => false
    }
  }

  val vertexLabels = vertexLabelRows.map(_.getString(1))
  val edgeLabels = edgeLabelRows.map(_.getString(1))


  val vertexScans = vertexLabels.map { label =>
    val data = session.sqlContext.read.cosmosDB(readConfig).where($"label" === functions.lit(label))
    val dataWithNewIds = data
      .withColumnRenamed("id", "cosmos_id")
      .withColumn("id", functions.monotonically_increasing_id())
      .drop("label")

    val relevantStructFields = data.schema.filter(!_.name.startsWith("_"))


    NodeScan.on("v" -> "id") {  builder =>
      val init = builder.build
        .withImpliedLabel(label)

      relevantStructFields.foldLeft(init) { (agg, field) =>
        agg.withPropertyKey(field.name)
      }
    }.from(CAPSRecords.create(dataWithNewIds))
  }

  vertexScans.foreach(println(_))


}
