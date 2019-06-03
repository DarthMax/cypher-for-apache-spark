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
package org.opencypher.okapi.ir.impl

import cats._
import cats.data.State
import cats.data.State._
import cats.instances.list._
import cats.syntax.flatMap._
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, NotImplementedException}
import org.opencypher.okapi.impl.types.CypherTypeUtils._
import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.pattern._
import org.opencypher.okapi.ir.api.util.FreshVariableNamer
import org.opencypher.v9_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.opencypher.v9_0.expressions.{Expression, LogicalVariable, RelTypeName}
import org.opencypher.v9_0.{expressions => ast}

import scala.annotation.tailrec
import scala.util.Try

final class PatternConverter(irBuilderContext: IRBuilderContext) {

  type Result[A] = State[Pattern, A]

  def convert(
    p: ast.Pattern,
    knownTypes: Map[ast.Expression, CypherType],
    qualifiedGraphName: QualifiedGraphName,
    pattern: Pattern = Pattern.empty
  ): Pattern =
    convertPattern(p, knownTypes, qualifiedGraphName).runS(pattern).value

  def convertRelsPattern(
    p: ast.RelationshipsPattern,
    knownTypes: Map[ast.Expression, CypherType],
    qualifiedGraphName: QualifiedGraphName,
    pattern: Pattern = Pattern.empty
  ): Pattern =
    convertElement(p.element, knownTypes, qualifiedGraphName).runS(pattern).value

  private def convertPattern(
    p: ast.Pattern,
    knownTypes: Map[ast.Expression, CypherType],
    qualifiedGraphName: QualifiedGraphName
  ): Result[Unit] =
    Foldable[List].sequence_[Result, Unit](p.patternParts.toList.map(convertPart(knownTypes, qualifiedGraphName)))

  @tailrec
  private def convertPart(knownTypes: Map[ast.Expression, CypherType], qualifiedGraphName: QualifiedGraphName)
    (p: ast.PatternPart): Result[Unit] = p match {
    case _: ast.AnonymousPatternPart => stomp(convertElement(p.element, knownTypes, qualifiedGraphName))
    case ast.NamedPatternPart(_, part) => convertPart(knownTypes, qualifiedGraphName)(part)
  }

  private def convertElement(
    p: ast.PatternElement,
    knownTypes: Map[ast.Expression, CypherType],
    qualifiedGraphName: QualifiedGraphName
  ): Result[IRField] =
    p match {

      case np@ast.NodePattern(vOpt, labels: Seq[ast.LabelName], propertiesOpt, baseNodeVar) =>
        // labels within CREATE patterns, e.g. CREATE (a:Foo), labels for MATCH clauses are rewritten to WHERE
        val patternLabels = labels.map(_.name).toSet

        val baseNodeCypherTypeOpt = baseNodeVar.map(knownTypes)
        val maybeBaseNodeLabels = baseNodeCypherTypeOpt.map(_.toCTNode.labels)

        // labels defined in outside scope, passed in by IRBuilder
        val (maybeKnownLabels: Option[AnyOf], qgnOption: Option[QualifiedGraphName]) = vOpt.flatMap(expr => knownTypes.get(expr)) match {
          case Some(n: CTNode) => Some(n.labels) -> n.graph
          case _ => None -> None
        }

        val allLabels: AnyOf = (maybeKnownLabels, maybeBaseNodeLabels) match {
          case (Some(knownLabels), _) => knownLabels.addLabelsToAlternatives(patternLabels)
          case (None, Some(baseLabels)) => baseLabels.addLabelsToAlternatives(patternLabels)
          case _ if patternLabels.nonEmpty => AnyOf.combo(patternLabels)
          case _ => AnyOf.allLabels
        }

        val qgn = qgnOption.orElse(Some(irBuilderContext.workingGraph.qualifiedGraphName))

        val nodeVar = vOpt match {
          case Some(v) => Var(v.name)(CTNode(allLabels, Map.empty[String, CypherType], qgn))
          case None => FreshVariableNamer(np.position.offset, CTNode(allLabels, Map.empty[String, CypherType], qgn))
        }

        val baseNodeField = baseNodeVar.map(x => IRField(x.name)(knownTypes(x)))
        for {
          element <- pure(IRField(nodeVar.name)(nodeVar.cypherType))
          _ <- modify[Pattern](_.withElement(element, extractProperties(propertiesOpt)).withBaseField(element, baseNodeField))
        } yield element

      case rc@ast.RelationshipChain(left, ast.RelationshipPattern(eOpt, types, rangeOpt, propertiesOpt, dir, _, baseRelVar), right) =>

        val convertedProperties = extractProperties(propertiesOpt)
        val relVar = createRelationshipVar(knownTypes, rc.position.offset, eOpt, types, baseRelVar, qualifiedGraphName)

        val baseRelField = baseRelVar.map(x => IRField(x.name)(knownTypes(x)))

        for {
          source <- convertElement(left, knownTypes, qualifiedGraphName)
          target <- convertElement(right, knownTypes, qualifiedGraphName)
          rel <- pure(IRField(relVar.name)(if (rangeOpt.isDefined) CTList(relVar.cypherType) else relVar.cypherType))
          _ <- modify[Pattern] { given =>
            val registered = given
              .withElement(rel)
              .withBaseField(rel, baseRelField)

            rangeOpt match {
              case Some(Some(range)) =>
                val lower = range.lower.map(_.value.intValue()).getOrElse(1)
                val upper = range.upper
                  .map(_.value.intValue())
                  .getOrElse(throw NotImplementedException("Support for unbounded var-length not yet implemented"))
                val relType = relVar.cypherType.toCTRelationship

                Endpoints.apply(source, target) match {
                  case _: IdenticalEndpoints =>
                    throw NotImplementedException("Support for cyclic var-length not yet implemented")

                  case ends: DifferentEndpoints =>
                    dir match {
                      case OUTGOING =>
                        registered.withConnection(rel, DirectedVarLengthRelationship(relType, ends, lower, Some(upper), OUTGOING), convertedProperties)

                      case INCOMING =>
                        registered.withConnection(rel, DirectedVarLengthRelationship(relType, ends.flip, lower, Some(upper), INCOMING), convertedProperties)

                      case BOTH =>
                        registered.withConnection(rel, UndirectedVarLengthRelationship(relType, ends.flip, lower, Some(upper)), convertedProperties)
                    }
                }

              case None =>
                Endpoints.apply(source, target) match {
                  case ends: IdenticalEndpoints =>
                    registered.withConnection(rel, CyclicRelationship(ends), convertedProperties)

                  case ends: DifferentEndpoints =>
                    dir match {
                      case OUTGOING =>
                        registered.withConnection(rel, DirectedRelationship(ends, OUTGOING), convertedProperties)

                      case INCOMING =>
                        registered.withConnection(rel, DirectedRelationship(ends.flip, INCOMING), convertedProperties)

                      case BOTH =>
                        registered.withConnection(rel, UndirectedRelationship(ends), convertedProperties)
                    }
                }

              case _ => throw NotImplementedException(s"Support for pattern conversion of $rc not yet implemented")
            }
          }
        } yield target

      case x =>
        throw NotImplementedException(s"Support for pattern conversion of $x not yet implemented")
    }

  private def extractProperties(propertiesOpt: Option[Expression]) = {
    propertiesOpt.map(irBuilderContext.convertExpression) match {
      case Some(e: MapExpression) => Some(e)
      case Some(other) => throw IllegalArgumentException("MapExpression", other)
      case _ => None
    }
  }

  private def createRelationshipVar(
    knownTypes: Map[Expression, CypherType],
    offset: Int,
    eOpt: Option[LogicalVariable],
    types: Seq[RelTypeName],
    maybeBaseRel: Option[LogicalVariable],
    qualifiedGraphName: QualifiedGraphName
  ): Var = {

    val patternTypes = types.map(_.name).toSet

    val maybeBaseRelCypherType = maybeBaseRel.map(knownTypes)
    val baseRelTypes = maybeBaseRelCypherType.map(_.toCTRelationship.labels)

    // types defined in outside scope, passed in by IRBuilder
    val (knownRelTypes: Option[AnyOf], qgnOption: Option[QualifiedGraphName]) = eOpt.flatMap(expr => knownTypes.get(expr)) match {
      case Some(CTRelationship(t, _, qgn)) => Some(t) -> qgn
      case _ => None -> None
    }

    val qgn = qgnOption.orElse(Some(irBuilderContext.workingGraph.qualifiedGraphName))

    val relTypes = {
      if (patternTypes.nonEmpty) AnyOf.alternatives(patternTypes)
      else if (baseRelTypes.isDefined) baseRelTypes.get
      else knownRelTypes.getOrElse(AnyOf.allLabels)
    }

    val rel = eOpt match {
      case Some(v) => Var(v.name)(CTRelationship(relTypes, Map.empty[String, CypherType], qgn))
      case None => FreshVariableNamer(offset, CTRelationship(relTypes, Map.empty[String, CypherType], qgn))
    }
    rel
  }

  private def stomp[T](result: Result[T]): Result[Unit] = result >> pure(())
}
