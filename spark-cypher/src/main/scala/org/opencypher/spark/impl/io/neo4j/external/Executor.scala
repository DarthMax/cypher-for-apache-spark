/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.spark.impl.io.neo4j.external

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, types}
import org.neo4j.driver.internal.types.InternalTypeSystem
import org.neo4j.driver.internal.value.ListValue
import org.neo4j.driver.v1.types.{Type, TypeSystem}
import org.neo4j.driver.v1.{Driver, Session, StatementResult, Value}
import org.opencypher.spark.api.io.neo4j.Neo4jConfig
import org.opencypher.spark.impl.io.neo4j.Neo4jHelpers._

import scala.collection.JavaConverters._

private object Executor {

  def toJava(parameters: Map[String, Any]): java.util.Map[String, Object] = {
    parameters.mapValues(toJava).asJava
  }

  private def toJava(x: Any): AnyRef = x match {
    case y: Seq[_] => y.asJava
    case _ => x.asInstanceOf[AnyRef]
  }

  val EMPTY = Array.empty[Any]

  class Neo4jQueryResult(val schema: StructType, val rows: Iterator[Array[Any]]) {
    def sparkRows: Iterator[Row] = rows.map(row => new GenericRowWithSchema(row, schema))

    def fields: Array[String] = schema.fieldNames
  }

  private def rows(result: StatementResult) = {
    var i = 0
    while (result.hasNext) i = i + 1
    i
  }

  def execute(config: Neo4jConfig, query: String, parameters: Map[String, Any]): Neo4jQueryResult = {
    config.withSession { session =>
      val result: StatementResult = session.run(query, toJava(parameters))
      if (!result.hasNext) {
        return new Neo4jQueryResult(new StructType(), Iterator.empty)
      }
      val peek = result.peek()
      val keyCount = peek.size()
      if (keyCount == 0) {
        val res: Neo4jQueryResult =
          new Neo4jQueryResult(new StructType(), Array.fill[Array[Any]](rows(result))(EMPTY).toIterator)
        result.consume()
        return res
      }
      val keys = peek.keys().asScala
      val fields = keys.map(k => (k, peek.get(k).`type`())).map(keyType => CypherTypes.field(keyType))
      val schema = StructType(fields)

      val it = result.asScala.map { record =>
        val row = new Array[Any](keyCount)
        var i = 0
        while (i < keyCount) {
          row.update(i, convertLists(record.get(i)))
          i = i + 1
        }
        row
      }
      new Neo4jQueryResult(schema, it)
    }
  }

  private def convertLists(v: Value): AnyRef = v match {
    case list: ListValue =>
      list.asList().toArray
    case other => other.asObject()
  }
}

private object CypherTypes {
  val INTEGER: LongType.type = types.LongType
  val FlOAT: DoubleType.type = types.DoubleType
  val STRING: StringType.type = types.StringType
  val BOOLEAN: BooleanType.type = types.BooleanType
  val NULL: NullType.type = types.NullType

  def apply(typ: String): DataType = typ.toUpperCase match {
    case "LONG" => INTEGER
    case "INT" => INTEGER
    case "INTEGER" => INTEGER
    case "FLOAT" => FlOAT
    case "DOUBLE" => FlOAT
    case "NUMERIC" => FlOAT
    case "STRING" => STRING
    case "BOOLEAN" => BOOLEAN
    case "BOOL" => BOOLEAN
    case "NULL" => NULL
    case _ => STRING
  }

  def toSparkType(typeSystem: TypeSystem, typ: Type): org.apache.spark.sql.types.DataType =
    if (typ == typeSystem.BOOLEAN()) CypherTypes.BOOLEAN
    else if (typ == typeSystem.STRING()) CypherTypes.STRING
    else if (typ == typeSystem.INTEGER()) CypherTypes.INTEGER
    else if (typ == typeSystem.FLOAT()) CypherTypes.FlOAT
    else if (typ == typeSystem.NULL()) CypherTypes.NULL
    else CypherTypes.STRING

  def field(keyType: (String, Type)): StructField = {
    StructField(keyType._1, CypherTypes.toSparkType(InternalTypeSystem.TYPE_SYSTEM, keyType._2))
  }

  def schemaFromNamedType(schemaInfo: Seq[(String, String)]): StructType = {
    val fields = schemaInfo.map(field => StructField(field._1, CypherTypes(field._2), nullable = true))
    StructType(fields)
  }

  def schemaFromDataType(schemaInfo: Seq[(String, types.DataType)]): StructType = {
    val fields = schemaInfo.map(field => StructField(field._1, field._2, nullable = true))
    StructType(fields)
  }
}
