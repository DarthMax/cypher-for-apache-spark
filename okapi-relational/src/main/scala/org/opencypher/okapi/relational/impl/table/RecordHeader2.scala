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
package org.opencypher.okapi.relational.impl.table

import org.opencypher.okapi.api.types.{CTNode, CTRelationship, CypherType}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.RelType
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.impl.table.RichExpression._
import org.opencypher.okapi.trees.AbstractTreeNode

import scala.annotation.tailrec
import scala.reflect.ClassTag

object RecordHeader2 {
  def empty: RecordHeader2 = RecordHeader2(Set.empty)
}

case class RecordHeader2(entries: Set[RecordHeaderEntry]) {


  // Getter

  def column(expr: Expr)(implicit columnNameGenerator: List[RecordHeaderEntry] => String): String = {
    def findWithTrace(entry: RecordHeaderEntry, stack: List[RecordHeaderEntry]): Option[List[RecordHeaderEntry]] = {
      if (entry.expressions.contains(expr)) Some(stack :+ entry)
      else entry.children.toStream
        .map(child => findWithTrace(child, stack :+ child))
        .collectFirst {
          case Some(list) => list
        }
    }

    entries.flatMap {
      entry => findWithTrace(entry, List.empty)
    }.headOption match {
      case Some(list) => columnNameGenerator(list)
      case None =>
        ???
    }
  }

  def columns: Set[String] = {
    entries.map {
      _.columnName
    }
  }

  def expressions: Set[Expr] = {
    val exprs = entries.map {
      _.transform[Set[Expr]] {
        case (entry, innerExprs) => entry.expressions ++ innerExprs.flatten
      }
    }
    exprs.flatten
  }

  def vars: Set[Var] = {
    entries.flatMap { entry =>
      entry.expressions.collect {
        case v: Var => v
      }
    }
  }

  def returnItems = {
    vars.collect {
      case r: ReturnItem => r
    }
  }


  // modification

  def select[T <: Expr](exprs: T*): RecordHeader2 = select(exprs.toSet)

  def select[T <: Expr](exprs: Set[T]): RecordHeader2 = {
    val aliasExprs = exprs.collect { case a: AliasExpr => a }
    val headerWithAliases = withAlias(aliasExprs.toSeq: _*)

    val selectExpressions = exprs.flatMap { e: Expr =>
      e match {
        case v: Var => v
        case AliasExpr(expr, alias) => alias
        case nonVar => ???
      }
    }

    val selectEntries = selectExpressions
      .groupBy { headerWithAliases.findEntryByExpr }
      .flatMap {
        case (None, _) => None
        case (Some(entry), aliasesToKeep) => entry.dropAliases(entry.expressions -- aliasesToKeep)
      }.toSet

    RecordHeader2(selectEntries)
  }

  def join(other: RecordHeader2): RecordHeader2 = {
    val leftTopLevelExprs = entries.map(_.expressions)
    val rightTopLevelExprs = other.entries.map(_.expressions)

    val expressionOverlap = leftTopLevelExprs.intersect(rightTopLevelExprs)
    if (expressionOverlap.nonEmpty) {
      throw IllegalArgumentException("two headers with non overlapping expressions", s"overlapping expressions: $expressionOverlap")
    }

    val columnOverlap = columns intersect other.columns
    if (columnOverlap.nonEmpty) {
      throw IllegalArgumentException("two headers with non overlapping columns", s"overlapping columns: $columnOverlap")
    }

    this ++ other
  }

  def withAlias(aliases: AliasExpr*): RecordHeader2 = aliases.foldLeft(this) {
    case (currentHeader, alias) => currentHeader.withAlias(alias)
  }

  def withAlias(expr: AliasExpr): RecordHeader2 = {
    val from = expr.expr
    val to = expr.alias

    val fromEntry = findEntryByExpr(from) match {
      case Some(e) => e
      case None => throw IllegalArgumentException(s"An expression in $this", s"Unknown expression $from")
    }

    if (contains(to) && !(from == to)) throw IllegalArgumentException(s"A none existing expression", s"$to present in ${this.expressions}")

    val newEntry = from match {
      case _: Var =>
        fromEntry.rewrite {
          case entry =>
            val aliasExpr = entry.expressions.head.withOwner(to)
            entry.copy(expressions = expressions - aliasExpr + aliasExpr)
        }
      case _ => RecordHeaderEntry(to, to.name, fromEntry.inner: _*)
    }

    withEntry(newEntry)
  }

  def --(toDrop: Set[Expr]): RecordHeader2 = {
    val toDropWithChildren = toDrop.flatMap(expressionsFor)

    val mappedTree = entries
      .flatMap(_.dropAliases(toDrop))
      .map { root =>
        root.rewriteTopDown {
          case entry: RecordHeaderEntry if (entry.expressions intersect toDropWithChildren).nonEmpty =>
            entry.copy(inner = entry.inner.flatMap(_.dropAliases(toDropWithChildren)))
        }
      }

    RecordHeader2(mappedTree)
  }

  def withExpr(expr: Expr): RecordHeader2 = {
    val newEntry = RecordHeaderEntry(expr, expr.columnName)

    expr match {
      case _ if contains(expr) => this
      case alias: AliasExpr => withAlias(alias)
      case _: Var => copy(entries = entries ++ newEntry)
      case _ if expr.owner.isDefined && contains(expr.owner.get) =>
        val parent = expr.owner.get

        val mappedTree = entries.map { root =>
          root.rewrite {
            case entry if entry.expressions.contains(parent) => entry.addChild(newEntry)
          }
        }
        copy(entries = mappedTree)

      case _ => throw IllegalArgumentException(s"A Var or an expression with an owner present in $expressions", expr)
    }
  }

  def withExprs(exprs: Set[Expr]): RecordHeader2 = {
    val (vars, other) = exprs.partition {
      case _: Var => true
      case _ => false
    }

    val withAddedVars = vars.foldLeft(this)(_.withExpr(_))
    other.foldLeft(withAddedVars)(_.withExpr(_))
  }

  private def withEntry(entry: RecordHeaderEntry) = {
    val filtered = entries.filterNot(other => (other.expressions intersect entry.expressions).nonEmpty)

    copy(entries = filtered + entry)
  }

  private[relational] def ++(other: RecordHeader2): RecordHeader2 = {
    val result = (entries ++ other.entries).map {
      case entry@RecordHeaderEntry(expr, columnName, inner) =>

        val leftCT = entries.find(_.expressions == expr)
        val rightCT = other.entries.find(_.expressions == expr)

        (leftCT, rightCT) match {
          case (Some(left), None) => left
          case (None, Some(right)) => right
          case (Some(left), Some(right)) => left join right
          case _ => throw new IllegalStateException
        }
    }
    copy(entries = result)
  }

  def getEntryByExpr(expr: Expr): RecordHeaderEntry = {
    findEntryByExpr(expr).getOrElse(throw IllegalArgumentException(s"$expr to be present", expressions))
  }

  def findEntryByExpr(expr: Expr): Option[RecordHeaderEntry] = {
    entries.flatMap { entry =>
      entry.find { e =>
        e.expressions.contains(expr)
      }
    }.headOption
  }

  def contains(expr: Expr): Boolean = {
    entries.exists { entry =>
      entry.exists(_.expressions.contains(expr))
    }
  }

  def isEmpty: Boolean = entries.isEmpty

  def expressionsFor(expr: Expr): Set[Expr] = {
    findEntryByExpr(expr) match {
      case Some(entry) => entry.inner.flatMap(_.transform[Set[Expr]] { case (e, exprs) =>
        exprs.flatten.toSet ++ e.expressions
      }).toSet + expr
      case None => Set.empty
    }
  }

  def aliasesFor(v: Var): Set[Expr] = {
    findEntryByExpr(v).map(_.expressions).getOrElse(Set.empty)
  }

  def idExpressions(): Set[Expr] = {
    entries.flatMap { entry =>
      entry.transform[Set[Expr]] {
        case (e, innerExprs) =>
          e.expressions.filter {
            case _: Id | _: Identity | _: StartNode | _: EndNode => true
            case _ => false
          } ++ innerExprs.flatten
      }
    }
  }

  def idExpressions(v: Var): Set[Expr] = {
    val aliases = aliasesFor(v)

    idExpressions().filter(_.owner.exists(aliases.contains(_)))
  }

  def propertiesFor(expr: Expr): Set[Property] = expressionsOfType[Property](expr)
  def labelsFor(expr: Expr): Set[HasLabel] = expressionsOfType[HasLabel](expr)
  def typesFor(expr: Expr): Set[HasType] = expressionsOfType[HasType](expr)
  def startNodeFor(expr: Expr): StartNode = expressionsOfType[StartNode](expr).head
  def endNodeFor(expr: Expr): EndNode = expressionsOfType[EndNode](expr).head

  private def expressionsOfType[T <: Expr : ClassTag](expr: Expr): Set[T] = {
    expressionsFor(expr).collect {
      case p: T => p
    }
  }

  def entityVars: Set[Var] = nodeVars ++ relationshipVars

  def nodeVars[T >: NodeVar <: Var]: Set[T] = {
    entries.flatMap {
      _.expressions.collect {
        case v: NodeVar => v
      }
    }
  }

  def nodeEntities: Set[Var] = {
    entries.flatMap {
      _.expressions.collect {
        case v: Var if v.cypherType.subTypeOf(CTNode) => v
      }
    }
  }

  def relationshipVars[T >: RelationshipVar <: Var]: Set[T] = {
    entries.flatMap {
      _.expressions.collect {
        case v: RelationshipVar => v
      }
    }
  }

  def relationshipEntities: Set[Var] = {
    entries.flatMap {
      _.expressions.collect {
        case v: Var if v.cypherType.subTypeOf(CTRelationship) => v
      }
    }
  }

  def entitiesForType(ct: CypherType, exactMatch: Boolean = false): Set[Var] = {
    ct match {
      case n: CTNode => nodesForType(n, exactMatch)
      case r: CTRelationship => relationshipsForType(r)
      case other => throw IllegalArgumentException("Entity", other)
    }
  }

  def nodesForType[T >: NodeVar <: Var](nodeType: CTNode, exactMatch: Boolean = false): Set[T] = {
    // and semantics
    val requiredLabels = nodeType.labels

    nodeVars[T].filter { nodeVar =>
      val physicalLabels = labelsFor(nodeVar).map(_.label.name)
      val logicalLabels = nodeVar.cypherType match {
        case CTNode(labels, _) => labels
        case _ => Set.empty[String]
      }
      if (exactMatch) {
        requiredLabels == (physicalLabels ++ logicalLabels)
      } else {
        requiredLabels.subsetOf(physicalLabels ++ logicalLabels)
      }
    }
  }

  def relationshipsForType[T >: RelationshipVar <: Var](relType: CTRelationship): Set[T] = {
    // or semantics
    val possibleTypes = relType.types

    relationshipVars[T].filter { relVar =>
      val physicalTypes = typesFor(relVar).map {
        case HasType(_, RelType(name)) => name
      }
      val logicalTypes = relVar.cypherType match {
        case CTRelationship(types, _) => types
        case _ => Set.empty[String]
      }
      possibleTypes.isEmpty || (physicalTypes ++ logicalTypes).exists(possibleTypes.contains)
    }
  }

//  private[opencypher] def newConflictFreeColumnName(expr: Expr, usedColumnNames: Set[String] = columns): String = {
//    @tailrec def recConflictFreeColumnName(candidateName: String): String = {
//      if (usedColumnNames.contains(candidateName)) recConflictFreeColumnName(s"_$candidateName")
//      else candidateName
//    }
//
//    val firstColumnNameCandidate = expr.toString
//      .replaceAll("-", "_")
//      .replaceAll(":", "_")
//      .replaceAll("\\.", "_")
//    recConflictFreeColumnName(firstColumnNameCandidate)
//  }
}

case class RecordHeaderEntry(expressions: Set[Expr], columnName: String, inner: List[RecordHeaderEntry]) extends AbstractTreeNode[RecordHeaderEntry] {
  require(expressions.nonEmpty)

  def cypherType: CypherType = expressions.map(_.cypherType).reduce(_ join _)

  def dropAliases(toDrop: Set[Expr]): Option[RecordHeaderEntry] = {
    val filtered = expressions -- toDrop

    if (filtered.isEmpty) None else Some(copy(expressions = filtered))
  }

  def addChild(child: RecordHeaderEntry): RecordHeaderEntry = {
    if (inner.exists(entry => child.expressions.subsetOf(entry.expressions))) {
      this
    } else {
      copy(inner = child :: inner)
    }
  }

  def join(other: RecordHeaderEntry): RecordHeaderEntry = {
    if (expressions != other.expressions) throw IllegalArgumentException("Entries with equal expressions", Seq(expressions, other.expressions))
    if (columnName != other.columnName) throw IllegalArgumentException("Entries with equal columnNames", Seq(columnName, other.columnName))

    val joinedExpression = (expressions ++ other.expressions).map { expression =>
      (expression, cypherType, other.cypherType) match {
        case (v: Var, l: CTNode, r: CTNode) => Var(v.name)(l.join(r))
        case (v: Var, l: CTRelationship, r: CTRelationship) => Var(v.name)(l.join(r))
        case (_, l, r) if l.subTypeOf(r) => other.expressions.find(_ == expression).get
        case (_, l, r) if r.subTypeOf(l) => expressions.find(_ == expression).get
        case (e, lCT, rCT) => throw IllegalArgumentException(
          expected = s"Compatible Cypher types for expression $e",
          actual = s"left type `$lCT` and right type `$rCT`"
        )
      }
    }

    val joinedChildren = (inner ++ other.inner).map {
      case RecordHeaderEntry(expr, _, _) =>

        val leftCT = inner.find(_.expressions == expr)
        val rightCT = other.inner.find(_.expressions == expr)

        (leftCT, rightCT) match {
          case (Some(left), None) => left
          case (None, Some(right)) => right
          case (Some(left), Some(right)) => left join right
          case _ => throw new IllegalStateException
        }
    }

    RecordHeaderEntry(joinedExpression, columnName, joinedChildren)
  }
}

case object RecordHeaderEntry {
  def apply[T <: Expr](expressions: Set[T], columnName: String, inner: Seq[RecordHeaderEntry]): RecordHeaderEntry = {
    RecordHeaderEntry(expressions.asInstanceOf[Set[Expr]], columnName, inner.toList)
  }

  def apply(expression: Expr, columnName: String, inner: RecordHeaderEntry*): RecordHeaderEntry = {
    RecordHeaderEntry(Set(expression), columnName, inner.toList)
  }
}

object RichExpression {
  implicit class ExpressionColumnName(expr: Expr) {
    def columnName: String = expr match {
      case _: Identity => "id"
      case v: Var => v.name
      case HasLabel(_, label) => s":${label.name}"
      case HasType(_, typ) => s":${typ.name}"
      case _: StartNode => "source"
      case _: EndNode => "target"
      case p: Property => s"property_${p.key.name}"
      case _ => throw IllegalArgumentException("", expr)
    }
  }
}
