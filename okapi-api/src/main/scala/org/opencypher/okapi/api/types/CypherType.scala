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
package org.opencypher.okapi.api.types

import cats.Monoid
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.impl.types.CypherTypeParser
import upickle.default._

trait CypherType {

  def isNullable: Boolean = false

  def containsNullable: Boolean = isNullable

  def asNullableAs(other: CypherType): CypherType = {
    if (!isNullable && other.isNullable) {
      nullable
    } else if (isNullable && !other.isNullable) {
      material
    } else {
      this
    }
  }

  def material: CypherType = this

// TODO Do we need these functions
//  def &(other: CypherType): CypherType = meet(other)
//
//  def meet(other: CypherType): CypherType = {
//    if (this.subTypeOf(other)) this
//    else if (other.subTypeOf(this)) other
//    else {
//      this -> other match {
//        case (l: CTNode, r: CTNode) if l.graph == r.graph => CTNode(l.labels ++ r.labels, l.graph)
//        case (l: CTNode, r: CTNode) => CTNode(l.labels ++ r.labels)
////        case (l: CTRelationship, r: CTRelationship) =>
////          val types = l.types.intersect(r.types)
////          if (types.isEmpty) CTVoid
////          else if (l.graph == r.graph) CTRelationship(types, l.graph)
////          else CTRelationship(types)
//        case (CTList(l), CTList(r)) => CTList(l & r)
//        case (CTUnion(ls), CTUnion(rs)) => CTUnion({
//          for {
//            l <- ls
//            r <- rs
//          } yield l & r
//        }.toSeq: _*)
//        case (CTUnion(ls), r) => CTUnion(ls.map(_ & r).toSeq: _*)
//        case (l, CTUnion(rs)) => CTUnion(rs.map(_ & l).toSeq: _*)
//        case (CTMap(pl), CTMap(pr)) =>
//          val intersectedProps = (pl.keys ++ pr.keys).map { k =>
//            val ct = pl.get(k) -> pr.get(k) match {
//              case (Some(tl), Some(tr)) => tl | tr
//              case (Some(tl), None) => tl.nullable
//              case (None, Some(tr)) => tr.nullable
//              case (None, None) => CTVoid
//            }
//            k -> ct
//          }.toMap
//          CTMap(intersectedProps)
//        case (_, _) => CTVoid
//      }
//    }
//  }
//
//  def intersects(other: CypherType): Boolean = meet(other) != CTVoid

  lazy val nullable: CypherType = {
    if (isNullable) this
    else CTUnion(this, CTNull)
  }

  def |(other: CypherType): CypherType = join(other)

  def join(other: CypherType): CypherType = {
    if (this.subTypeOf(other)) other
    else if (other.subTypeOf(this)) this
    else {
      this -> other match {
        case (l: CTElement, r: CTElement) if l.getClass == r.getClass =>
          val labels = l.labels ++ r.labels
          val properties = (l.properties.keys ++ r.properties.keys).map { key =>
            key -> (l.properties.getOrElse(key, CTNull) | r.properties.getOrElse(key, CTNull))
          }.toMap
          val maybeGraph = l.graph.orElse(r.graph)

          l match {
            case _: CTNode => CTNode(labels, properties, maybeGraph)
            case _: CTRelationship => CTRelationship(labels, properties, maybeGraph)
          }
        case (CTBigDecimal(lp, ls), CTBigDecimal(rp, rs)) =>
          val maxScale = Math.max(ls, rs)
          val maxDiff = Math.max(lp - ls, rp - rs)
          CTBigDecimal(maxDiff + maxScale, maxScale)
        case (CTUnion(ls), CTUnion(rs)) => CTUnion(ls ++ rs)
        case (CTUnion(ls), r) => CTUnion(r +: ls.toSeq: _*)
        case (l, CTUnion(rs)) => CTUnion(l +: rs.toSeq: _*)
        case (l, r) => CTUnion(l, r)
      }
    }
  }
  def superTypeOf(other: CypherType): Boolean = other.subTypeOf(this)

  def subTypeOf(other: CypherType): Boolean = {
    this -> other match {
      case (CTVoid, _) => true
      case (l, r) if l == r => true
      case (_, CTAny) => true
      case (_: CTBigDecimal, CTBigDecimal) => true
      case (CTBigDecimal, _: CTBigDecimal) => false
      case (CTBigDecimal(lp, ls), CTBigDecimal(rp, rs)) => (lp <= rp) && (ls <= rs) && (lp - ls <= rp - rs)
      case (l, CTAnyMaterial) if !l.isNullable => true
      case (_: CTMap, CTMap) => true
      case (l: CTElement, r: CTElement) if l.getClass == r.getClass =>
        l.graph == r.graph &&
        l.labels.subsetOf(r.labels) &&
        (l.properties.keys ++ r.properties.keys).forall { key =>
            l.properties.getOrElse(key, CTNull).subTypeOf(r.properties.getOrElse(key, CTNull))
        }
      case (CTUnion(las), r: CTUnion) => las.forall(_.subTypeOf(r))
      case (l, CTUnion(ras)) => ras.exists(l.subTypeOf)
      case (CTList(l), CTList(r)) => l.subTypeOf(r)
      case (l@CTMap(lps), CTMap(rps)) =>
        if (l == CTMap) false
        else {
          (lps.keySet ++ rps.keySet).forall { key =>
            lps.getOrElse(key, CTNull).subTypeOf(rps.getOrElse(key, CTNull))
          }
        }
      case _ => false
    }
  }

  def couldBeSameTypeAs(other: CypherType): Boolean = {
    this.subTypeOf(other) || other.subTypeOf(this)
  }

  def name: String = getClass.getSimpleName.filter(_ != '$').drop(2).toUpperCase

  override def toString: String = name

  def graph: Option[QualifiedGraphName] = None

  def withGraph(qgn: QualifiedGraphName): CypherType = this

  def withoutGraph: CypherType = this

}

object CypherType {

  /**
    * Parses the name of CypherType into the actual CypherType object.
    *
    * @param name string representation of the CypherType
    * @return
    * @see {{{org.opencypher.okapi.api.types.CypherType#name}}}
    */
  def fromName(name: String): Option[CypherType] = CypherTypeParser.parseCypherType(name)

  implicit val typeRw: ReadWriter[CypherType] = readwriter[String].bimap[CypherType](_.name, s => fromName(s).get)

  implicit val joinMonoid: Monoid[CypherType] = new Monoid[CypherType] {
    override def empty: CypherType = CTVoid

    override def combine(x: CypherType, y: CypherType): CypherType = x | y
  }

}

case object CTAnyMaterial extends CypherType {
  override lazy val nullable: CypherType = CTAny

  override def name: String = "ANY"
}

object CTMap extends CTMap(Map.empty) {
  override def name: String = "MAP"

  override def equals(obj: Any): Boolean = obj.isInstanceOf[CTMap.type]

  override def canEqual(that: Any): Boolean = that.isInstanceOf[CTMap.type]

  def apply(propertyTypes: (String, CypherType)*): CTMap = CTMap(propertyTypes.toMap)

}

case class CTMap(properties: Map[String, CypherType] = Map.empty) extends CypherType {

  override def containsNullable: Boolean = properties.values.exists(_.containsNullable)

  override def name: String = {
    s"MAP(${properties.map { case (n, t) => s"$n: ${t.name}" }.mkString(", ")})"
  }

}

object CTList extends CTList(CTAny) {

  override def name: String = "LIST"

}

case class CTList(inner: CypherType) extends CypherType {

  override def containsNullable: Boolean = inner.containsNullable

  override def name: String = s"LIST(${inner.name})"

}

/**
  * Represents a set of labels which an entity definitely carries
  */
case class AllOf(combo: Set[String]) {
  override def toString(): String = s"AllOf(${combo.mkString(", ")})"

  def addLabels(labels: Set[String]): AllOf = {
    copy(combo = combo ++ labels)
  }

  def size: Int = combo.size

  def subsetOf(other: AllOf): Boolean = if(combo.isEmpty || other.combo.isEmpty) {
    combo == other.combo
  } else {
    combo.subsetOf(other.combo)
  }
}

object AllOf {
  def apply(combo: String*): AllOf = AllOf(combo.toSet)
  def empty = AllOf(Set.empty[String])
}

/**
  * Represents a set of label combination alternatives, one of which an element definitely carries
  */
case class AnyOf(alternatives: Set[AllOf]) {
  override def toString(): String = s"AnyOf(${alternatives.mkString(", ")})"

  def subsetOf(other: AnyOf): Boolean = alternatives.forall(alt => other.alternatives.exists(oalt => oalt.subsetOf(alt)))

  def addLabelsToAlternatives(labels: Set[String]): AnyOf = {
    this.copy(alternatives = alternatives.map(_.addLabels(labels)))
  }

  def unpack(): Set[Set[String]] = alternatives.map(_.combo)
  def unpackRelTypes(): Set[String] = alternatives.map(_.combo.head)

  def ++(other: AnyOf): AnyOf = AnyOf(alternatives ++ other.alternatives)

  def size: Int = alternatives.size

  def isEmpty: Boolean = ???
}

object AnyOf {

  def apply(labels: String*): AnyOf = alternatives(labels.toSet)

  /**
    * Constructs an [[AnyOf]] from a set of strings that are treated as a single label combination
    */
  def combo(combo: Set[String]): AnyOf = AnyOf(Set(AllOf(combo)))

  /**
    * Constructs an [[AnyOf]] from a set of strings that are treated as different label combinations consisting of
    * a single label.
    */
  def alternatives(labels: Set[String]): AnyOf = AnyOf(labels.map(combo => AllOf(Set(combo))))

  def allLabels: AnyOf = AnyOf(Set.empty[AllOf])
}

sealed trait CTElement extends CypherType {
//  require(labels.alternatives.nonEmpty, "Label alternatives cannot be empty")

  def labels: AnyOf
  def properties: Map[String, CypherType]
}

object CTNode {
  def fromCombo(
    labels: Set[String],
    properties: Map[String, CypherType],
    maybeGraph: Option[QualifiedGraphName] = None): CTNode =
    CTNode(AnyOf.combo(labels), properties, maybeGraph)

  def empty(labels: String*): CTNode = CTNode(AnyOf.combo(labels.toSet), Map.empty[String, CypherType], None)
}

case class CTNode(
  labels: AnyOf,
  properties: Map[String, CypherType],
  override val graph: Option[QualifiedGraphName] = None
) extends CTElement {

  override def withGraph(qgn: QualifiedGraphName): CTNode = copy(graph = Some(qgn))
  override def withoutGraph: CTNode = CTNode(labels, properties)

  //TODO adjust
  override def name: String =
      s"NODE(labels = $labels)${graph.map(g => s" @ $g").getOrElse("")}"

}

object CTRelationship {
  def apply(relTypes: String, properties: Map[String, CypherType]): CTRelationship =
    CTRelationship(AnyOf.combo(Set(relTypes)), properties)

  def empty(labels: String*): CTRelationship = {
    val foo = if (labels.isEmpty) AnyOf(Set(AllOf.empty)) else AnyOf.alternatives(labels.toSet)
    CTRelationship(foo, Map.empty[String, CypherType], None)
  }

  def fromAlternatives(
    labels: Set[String],
    properties: Map[String, CypherType],
    maybeGraph: Option[QualifiedGraphName] = None): CTRelationship =
    CTRelationship(AnyOf.alternatives(labels), properties, maybeGraph)
}

case class CTRelationship(
  labels: AnyOf,
  properties: Map[String, CypherType],
  override val graph: Option[QualifiedGraphName] = None
) extends CTElement {
//  require(labels.alternatives.forall(_.combo.nonEmpty), "A rel type must be set")

  override def withGraph(qgn: QualifiedGraphName): CTRelationship = copy(graph = Some(qgn))
  override def withoutGraph: CTRelationship = CTRelationship(labels, properties)

  override def name: String =
      s"RELATIONSHIP(types = $labels)${graph.map(g => s" @ $g").getOrElse("")}"

}

case object CTString extends CypherType

case object CTInteger extends CypherType

case object CTFloat extends CypherType

case object CTTrue extends CypherType

case object CTFalse extends CypherType

case object CTNull extends CypherType {
  override def isNullable: Boolean = true
  override def material: CypherType = CTVoid
}

case object CTIdentity extends CypherType

case object CTLocalDateTime extends CypherType

case object CTDate extends CypherType

case object CTDuration extends CypherType

case object CTVoid extends CypherType

case class CTUnion(alternatives: Set[CypherType]) extends CypherType {
  require(!alternatives.exists(_.isInstanceOf[CTUnion]), "Unions need to be flattened")

  override def isNullable: Boolean = alternatives.contains(CTNull)

  override def material: CypherType = CTUnion((alternatives - CTNull).toSeq: _*)

  override def name: String = {
    if (this == CTAny) "ANY?"
    else if (this == CTBoolean) "BOOLEAN"
    else if (isNullable) s"${material.name}?"
    else if (subTypeOf(CTNumber)) "NUMBER"
    else s"UNION(${alternatives.mkString(", ")})"
  }

  override def graph: Option[QualifiedGraphName] = alternatives.flatMap(_.graph).headOption

}

object CTUnion {
  def apply(ts: CypherType*): CypherType = {
    val flattened = ts.flatMap {
      case u: CTUnion => u.alternatives
      case p => Set(p)
    }.distinct.toList

    // Filter alternatives that are a subtype of another alternative
    val filtered = flattened.filter(t => !flattened.exists(o => o != t && t.subTypeOf(o)))

    filtered match {
      case Nil => CTVoid
      case h :: Nil => h
      case many if many.contains(CTAnyMaterial) => if (many.contains(CTNull)) CTAny else CTAnyMaterial
      case many => CTUnion(many.toSet)
    }
  }
}

case object CTPath extends CypherType

object CTBigDecimal extends CTBigDecimal(-1, -1) {
  def apply(precisionAndScale: (Int, Int)): CTBigDecimal =
    CTBigDecimal(precisionAndScale._1, precisionAndScale._2)

  override def name: String = "BIGDECIMAL"
}

case class CTBigDecimal(precision: Int = 10, scale: Int = 0) extends CypherType {

  override def name: String = s"BIGDECIMAL($precision,$scale)"

}
