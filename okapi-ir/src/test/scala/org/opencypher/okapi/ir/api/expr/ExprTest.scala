/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.okapi.ir.api.expr

import org.opencypher.okapi.api.types._
import org.opencypher.okapi.testing.BaseTestSuite
import org.opencypher.okapi.testing.MatchHelper.equalWithTracing
import org.opencypher.okapi.impl.exception._

class ExprTest extends BaseTestSuite {

  test("expressions ignore cypher type in equality") {
    val n = Var("a")(CTInteger)
    val r = Var("a")(CTString)
    n should equal(r)

    val a = StartNode(Var("rel")(CTRelationship.empty))(CTAny)
    val b = StartNode(Var("rel")(CTRelationship.empty))(CTNode.empty)
    a should equal(b)
  }

  test("same expressions with different cypher types have the same hash code") {
    val n = Var("a")(CTNode("a", Map.empty[String, CypherType]))
    val r = Var("a")(CTRelationship("b", Map.empty[String, CypherType]))
    n.hashCode should equal(r.hashCode)

    val a = StartNode(Var("rel")(CTRelationship.empty))(CTAny)
    val b = StartNode(Var("rel")(CTRelationship.empty))(CTNode.empty)
    a.hashCode should equal(b.hashCode)
  }

  test("different expressions are not equal") {
    val p = Param("a")(CTAny)
    val v = Var("a")()
    p should not equal v
  }

  test("different expressions have different hash codes") {
    val p = Param("a")(CTAny)
    val v = Var("b")()
    p.hashCode should not equal v.hashCode
  }

  test("alias expression has updated type") {
    val n = Var("n")(CTNode.empty)
    val aliasVar = Var("m")()
    (n as aliasVar).cypherType should equal(aliasVar.cypherType)
  }

  describe("CypherType computation") {
    val a = Var("a")(CTNode.empty)
    val b = Var("b")(CTUnion(CTInteger, CTString))
    val c = Var("c")(CTUnion(CTInteger, CTString.nullable))
    val d = Var("d")(CTInteger.nullable)
    val e = Var("e")(CTString.nullable)
    val datetime = Var("datetime")(CTLocalDateTime)
    val duration = Var("duration")(CTDuration)
    val number = Var("number")(CTNumber)

    it("types Coalesce correctly") {
      Coalesce(List(a, b)).cypherType should equal(CTUnion(CTNode.empty, CTInteger, CTString))
      Coalesce(List(b, c)).cypherType should equal(CTUnion(CTInteger, CTString))
      Coalesce(List(a, b, c, d)).cypherType should equal(CTUnion(CTNode.empty, CTInteger, CTString))

      Coalesce(List(d,e)).cypherType should equal(CTUnion(CTInteger, CTString).nullable)
    }

    it("types ListSegment correctly") {
      ListSegment(3, Var("list")(CTList(CTNode.empty))).cypherType should equalWithTracing(
        CTNode.empty.nullable
      )

      ListSegment(3, Var("list")(CTUnion(CTList(CTString), CTList(CTInteger)))).cypherType should equalWithTracing(
        CTUnion(CTString, CTInteger).nullable
      )

      ListSegment(3, Var("list")(CTNull)).cypherType should equalWithTracing(
        CTNull
      )
    }

    it("types MapExpression correctly") {
      val mapFields = Map(
        "a" -> a,
        "b" -> b,
        "c" -> c,
        "d" -> d
      )
      MapExpression(mapFields).cypherType should equalWithTracing(CTMap(mapFields.mapValues(_.cypherType)))
    }

    it("types ListLit correctly") {
      ListLit(List(a, b)).cypherType should equal(CTList(CTUnion(CTNode.empty, CTInteger, CTString)))
      ListLit(List(b, c)).cypherType should equal(CTList(CTUnion(CTInteger, CTString.nullable)))
      ListLit(List(a, b, c, d)).cypherType should equal(CTList(CTUnion(CTNode.empty, CTInteger, CTString).nullable))
    }

    it("types Explode correctly") {
      Explode(Var("list")(CTList(CTNode.empty))).cypherType should equalWithTracing(
        CTNode
      )

      Explode(Var("list")(CTUnion(CTList(CTString), CTList(CTInteger)))).cypherType should equalWithTracing(
        CTUnion(CTString, CTInteger)
      )

      Explode(Var("list")(CTNull)).cypherType should equalWithTracing(
        CTVoid
      )
    }

    it("types Avg correctly") {
      Avg(duration).cypherType shouldBe CTDuration
      Avg(number).cypherType shouldBe CTNumber
      an[UnsupportedOperationException] shouldBe thrownBy {Avg(datetime).cypherType}
    }

    it("types Sum correctly") {
      Sum(duration).cypherType shouldBe CTDuration
      Sum(number).cypherType shouldBe CTNumber
      an[UnsupportedOperationException] shouldBe thrownBy {Sum(datetime).cypherType}
    }
  }

}
