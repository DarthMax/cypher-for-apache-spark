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
package org.opencypher.okapi.relational.impl.table

import org.opencypher.okapi.api.types.{CTBoolean, CTInteger, CTNode, CTString}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.{Label, PropertyKey}
import org.opencypher.okapi.ir.test.support.MatchHelper._
import org.scalatest.{FunSpec, Matchers}

class RecordHeaderNewTest extends FunSpec with Matchers {

  val n: Var = Var("n")(CTNode)
  val m: Var = Var("m")(CTNode)
  val o: Var = Var("o")(CTNode)

  val countN = CountStar(CTInteger)

  val nLabelA: HasLabel = HasLabel(n, Label("A"))(CTBoolean)
  val nLabelB: HasLabel = HasLabel(n, Label("B"))(CTBoolean)
  val nPropFoo: Property = Property(n, PropertyKey("foo"))(CTString)
  val nExprs: Set[Expr] = Set(n, nLabelA, nLabelB, nPropFoo)
  val mExprs: Set[Expr] = nExprs.map(_.withOwner(m))
  val oExprs: Set[Expr] = nExprs.map(_.withOwner(o))

  val nHeader: RecordHeaderNew = RecordHeaderNew.empty.withExprs(nExprs)
  val mHeader: RecordHeaderNew = RecordHeaderNew.empty.withExprs(mExprs)

  it("can add an entity expression") {
    nHeader.ownedBy(n) should equal(nExprs)
  }

//  it("can add a non-entity expression") {
//    val header = RecordHeaderNew.empty.withExpr(countN)
//
//    header.column(countN)
//  }

  it("can add an alias for an entity") {
    val withAlias = nHeader.withAlias(m, n)

    withAlias.ownedBy(n) should equalWithTracing(nExprs)
    withAlias.ownedBy(m) should equalWithTracing(mExprs)
  }

  it("can add an alias for a non-entity expression") {
    val withAlias1 = nHeader.withAlias(m, nPropFoo)
    val withAlias2 = withAlias1.withAlias(o, m)


    withAlias2.column(o) should equalWithTracing(withAlias2.column(nPropFoo))
    withAlias2.column(m) should equalWithTracing(withAlias2.column(nPropFoo))
    withAlias2.ownedBy(n) should equalWithTracing(nExprs)
    withAlias2.ownedBy(o) should equalWithTracing(Set.empty)
    withAlias2.ownedBy(m) should equalWithTracing(Set.empty)
  }

  it("can combine simple headers") {
    val unionHeader = nHeader ++ mHeader

    unionHeader.ownedBy(n) should equalWithTracing(nExprs)
    unionHeader.ownedBy(m) should equalWithTracing(mExprs)
  }

  it("can combine complex headers") {
    val p = Var("nPropFoo_Alias")(nPropFoo.cypherType)

    val nHeaderWithAlias = nHeader.withAlias(p, nPropFoo)
    val mHeaderWithAlias = mHeader.withAlias(o, m)

    val unionHeader = nHeaderWithAlias ++ mHeaderWithAlias

    unionHeader.column(p) should equal(unionHeader.column(nPropFoo))
    unionHeader.ownedBy(n) should equalWithTracing(nExprs)
    unionHeader.ownedBy(m) should equalWithTracing(mExprs)
    unionHeader.ownedBy(o) should equalWithTracing(oExprs)
  }

}
