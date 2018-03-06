/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.okapi.ir.impl

import cats.implicits._
import org.atnos.eff._
import org.atnos.eff.all._
import org.neo4j.cypher.internal.frontend.v3_4.ast
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.util.v3_4.InputPosition
import org.neo4j.cypher.internal.v3_4.expressions.{Expression, StringLiteral, Variable, Pattern => AstPattern}
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, IllegalStateException, NotImplementedException}
import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.api.block.{SortItem, _}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.pattern.Pattern
import org.opencypher.okapi.ir.api.util.CompilationStage
import org.opencypher.okapi.ir.impl.refactor.instances._

object IRBuilder extends CompilationStage[ast.Statement, CypherQuery[Expr], IRBuilderContext] {

  override type Out = Either[IRBuilderError, (Option[CypherQuery[Expr]], IRBuilderContext)]

  override def process(input: ast.Statement)(implicit context: IRBuilderContext): Out =
    buildIR[IRBuilderStack[Option[CypherQuery[Expr]]]](input).run(context)

  override def extract(output: Out): CypherQuery[Expr] =
    output match {
      case Left(error)         => throw IllegalStateException(s"Error during IR construction: $error")
      case Right((Some(q), _)) => q
      case Right((None, _))    => throw IllegalStateException(s"Failed to construct IR")
    }

  private def buildIR[R: _mayFail: _hasContext](s: ast.Statement): Eff[R, Option[CypherQuery[Expr]]] =
    s match {
      case ast.Query(_, part) =>
        for {
          query <- {
            part match {
              case ast.SingleQuery(clauses) =>
                val blocks = clauses.toVector.traverse(convertClause[R])
                blocks >> convertRegistry

              case x =>
                error(IRBuilderError(s"Query not supported: $x"))(None)
            }
          }
        } yield query

      case x =>
        error(IRBuilderError(s"Statement not yet supported: $x"))(None)
    }

//  def registerIRGraph[R: _hasContext](c: Clause): Eff[R, IRGraph] = {
//    for {
//      context <- get[R, IRBuilderContext]
//      graph <- {
//        val currentGraph = context.semanticState.recordedContextGraphs.find {
//          case (clause, _) =>
//            c.position == clause.position
//        }.map(_._2)
//          .map { g =>
//            IRCatalogGraph(context.graphs(g.source), context.schemaFor(g.source))
//          }
//          .getOrElse(context.ambientGraph)
//        put[R, IRBuilderContext](context.withWorkingGraph(currentGraph)) >> pure[R, IRGraph](currentGraph)
//      }
//    } yield graph
//  }

  private def convertClause[R: _mayFail: _hasContext](c: ast.Clause): Eff[R, Vector[BlockRef]] = {

    c match {
      case ast.Match(optional, pattern, _, astWhere) =>
        for {
          pattern <- convertPattern(pattern)
          given <- convertWhere(astWhere)
          context <- get[R, IRBuilderContext]
          refs <- {
            val blockRegistry = context.blocks
            val after = blockRegistry.lastAdded.toSet
            val block = MatchBlock[Expr](after, pattern, given, optional, context.currentWorkingGraph)

            val typedOutputs = typedMatchBlock.outputs(block)
            val (ref, reg) = blockRegistry.register(block)
            val updatedContext = context.withBlocks(reg).withFields(typedOutputs)
            put[R, IRBuilderContext](updatedContext) >> pure[R, Vector[BlockRef]](Vector(ref))
          }
        } yield refs

      case ast.With(distinct, ast.ReturnItems(_, items), GraphReturnItems(_, gItems), orderBy, skip, limit, where)
          if !items.exists(_.expression.containsAggregate) =>
        for {
          fieldExprs <- items.toVector.traverse(convertReturnItem[R])
          graphs <- gItems.toVector.traverse(convertGraphReturnItem[R])
          given <- convertWhere(where)
          context <- get[R, IRBuilderContext]
          refs <- {
            val (projectRef, projectReg) =
              registerProjectBlock(context, fieldExprs, graphs, given, context.currentWorkingGraph, distinct = distinct)
            val appendList = (list: Vector[BlockRef]) => pure[R, Vector[BlockRef]](projectRef +: list)
            val orderAndSliceBlock = registerOrderAndSliceBlock(orderBy, skip, limit)
            put[R, IRBuilderContext](context.copy(blocks = projectReg)) >> orderAndSliceBlock flatMap appendList
          }
        } yield refs

      case ast.With(distinct, ast.ReturnItems(_, items), GraphReturnItems(_, Seq()), _, _, _, None) =>
        for {
          fieldExprs <- items.toVector.traverse(convertReturnItem[R])
          context <- get[R, IRBuilderContext]
          refs <- {
            val (agg, group) = fieldExprs.partition {
              case (_, _: Aggregator) => true
              case _                  => false
            }

            val (ref1, reg1) = registerProjectBlock(context, group, source = context.ambientGraph, distinct = distinct)
            val after = reg1.lastAdded.toSet
            val aggBlock =
              AggregationBlock[Expr](after, Aggregations(agg.toSet), group.map(_._1).toSet, context.ambientGraph)
            val (ref2, reg2) = reg1.register(aggBlock)

            put[R, IRBuilderContext](context.copy(blocks = reg2)) >> pure[R, Vector[BlockRef]](Vector(ref1, ref2))
          }
        } yield refs

      case ast.Unwind(listExpression, variable) =>
        for {
          tuple <- convertUnwindItem(listExpression, variable)
          context <- get[R, IRBuilderContext]
          block <- {
            val (list, item) = tuple

            val binds: UnwoundList[Expr] = UnwoundList(list, item)

            val block = UnwindBlock(context.blocks.lastAdded.toSet, binds, context.currentWorkingGraph)

            val (ref, reg) = context.blocks.register(block)

            put[R, IRBuilderContext](context.copy(blocks = reg)) >> pure[R, Vector[BlockRef]](Vector(ref))
          }
        } yield block

      case ast.Return(distinct, ast.ReturnItems(_, items), graphItems, orderBy, skip, limit, _) =>
        for {
          fieldExprs <- items.toVector.traverse(convertReturnItem[R])
          graphs <- convertGraphReturnItems(graphItems)
          context <- get[R, IRBuilderContext]
          refs <- {
            val (projectRef, projectReg) =
              registerProjectBlock(context, fieldExprs, distinct = distinct, source = context.currentWorkingGraph, graphs = graphs)
            val appendList = (list: Vector[BlockRef]) => pure[R, Vector[BlockRef]](projectRef +: list)
            val orderAndSliceBlock = registerOrderAndSliceBlock(orderBy, skip, limit)
            put[R, IRBuilderContext](context.copy(blocks = projectReg)) >> orderAndSliceBlock flatMap appendList
          }
          context2 <- get[R, IRBuilderContext]
          refs2 <- {
            val rItems = fieldExprs.map(_._1)
            val orderedFields = OrderedFieldsAndGraphs[Expr](rItems, graphs.toSet)
            val result = ResultBlock[Expr](Set(refs.last), orderedFields, Set.empty, Set.empty, context.ambientGraph)
            val (resultRef, resultReg) = context2.blocks.register(result)
            put[R, IRBuilderContext](context.copy(blocks = resultReg)) >> pure[R, Vector[BlockRef]](refs :+ resultRef)
          }
        } yield refs2

      case ast.Return(distinct, ast.DiscardCardinality(), graphItems, _, _, _, _) =>
        for {
          graphs <- convertGraphReturnItems(graphItems)
          context <- get[R, IRBuilderContext]
          refs <- {
            val (ref, reg) =
              registerProjectBlock(context, Vector.empty, distinct = distinct, source = context.currentWorkingGraph, graphs = graphs)
            val orderedFields = OrderedFieldsAndGraphs[Expr](Vector.empty, graphs.toSet)
            val returns = ResultBlock[Expr](Set(ref), orderedFields, Set.empty, Set.empty, context.ambientGraph)
            val (ref2, reg2) = reg.register(returns)
            put[R, IRBuilderContext](context.copy(blocks = reg2)) >> pure[R, Vector[BlockRef]](Vector(ref, ref2))
          }
        } yield refs

      case x =>
        error(IRBuilderError(s"Clause not yet supported: $x"))(Vector.empty[BlockRef])
    }
  }

  private def convertGraphReturnItems[R: _hasContext](maybeItems: Option[GraphReturnItems]): Eff[R, Vector[IRGraph]] = {

    maybeItems match {
      case None =>
        pure[R, Vector[IRGraph]](Vector.empty[IRGraph])
      case Some(GraphReturnItems(_, items)) =>
        for {
          graphs <- items.toVector.traverse(convertGraphReturnItem[R])
        } yield graphs
    }
  }

  private def registerProjectBlock(
      context: IRBuilderContext,
      fieldExprs: Vector[(IRField, Expr)],
      graphs: Seq[IRGraph] = Seq.empty,
      given: Set[Expr] = Set.empty[Expr],
      source: IRGraph,
      distinct: Boolean): (BlockRef, BlockRegistry[Expr]) = {
    val blockRegistry = context.blocks
    val binds = FieldsAndGraphs(fieldExprs.toMap, graphs.toSet)

    val after = blockRegistry.lastAdded.toSet
    val projs = ProjectBlock[Expr](after, binds, given, source, distinct)

    blockRegistry.register(projs)
  }

  private def registerOrderAndSliceBlock[R: _mayFail: _hasContext](
      orderBy: Option[OrderBy],
      skip: Option[Skip],
      limit: Option[Limit]) = {
    for {
      context <- get[R, IRBuilderContext]
      sortItems <- orderBy match {
        case Some(ast.OrderBy(sortItems)) =>
          sortItems.toVector.traverse(convertSortItem[R])
        case None => Vector[ast.SortItem]().traverse(convertSortItem[R])
      }
      skipExpr <- convertExpr(skip.map(_.expression))
      limitExpr <- convertExpr(limit.map(_.expression))

      refs <- {
        if (sortItems.isEmpty && skipExpr.isEmpty && limitExpr.isEmpty) pure[R, Vector[BlockRef]](Vector())
        else {
          val blockRegistry = context.blocks
          val after = blockRegistry.lastAdded.toSet

          val orderAndSliceBlock = OrderAndSliceBlock[Expr](after, sortItems, skipExpr, limitExpr, context.ambientGraph)
          val (ref, reg) = blockRegistry.register(orderAndSliceBlock)
          put[R, IRBuilderContext](context.copy(blocks = reg)) >> pure[R, Vector[BlockRef]](Vector(ref))
        }
      }
    } yield refs
  }

  private def convertGraphReturnItem[R: _hasContext](item: ast.GraphReturnItem): Eff[R, IRGraph] = item match {
    case ast.NewContextGraphs(source: GraphAtAs, target) if target.isEmpty || target.contains(source) =>
      convertSingleGraphAs[R](source)

    case ast.ReturnedGraph(graph) =>
      convertSingleGraphAs[R](graph)

    case _ => throw NotImplementedException(s"Support for setting a different target graph not yet implemented")
  }

  private def convertSingleGraphAs[R: _hasContext](graph: ast.SingleGraphAs): Eff[R, IRGraph] = ???
//  {
//    graph.as match {
//      case Some(Variable(graphName)) =>
//        for {
//          context <- get[R, IRBuilderContext]
//          result <- graph match {
//
//            case ast.GraphOfAs(astPattern, _, _) =>
//              for {
//                pattern <- convertPattern(astPattern)
//              } yield {
//                val schemaUnion = context.graphList.map(_.schema).reduce(_ ++ _)
//                val patternGraphSchema = schemaUnion.forPattern(pattern)
//                IRPatternGraph(patternGraphSchema, pattern)
//              }
//
//            case ast.GraphAtAs(url, _, _) =>
//
//              val qualifiedGraphNameString = url.url match {
//                case Left(_) =>
//                  throw NotImplementedException(s"Support for qualified graph names by parameter not yet implemented")
//                case Right(StringLiteral(literal)) => literal
//              }
//
//              val qualifiedGraphName = QualifiedGraphName(qualifiedGraphNameString)
//              val newContext = context.withGraphAt(graphName, qualifiedGraphName)
//              put[R, IRBuilderContext](newContext) >>
//                pure[R, IRGraph](IRCatalogGraph(qualifiedGraphName, newContext.schemaFor(graphName)))
//
//            case ast.GraphAs(ref, alias, _) if alias.isEmpty || alias.contains(ref) =>
//              pure[R, IRGraph](IRCatalogGraph(context.graphs(graphName), context.schemaFor(graphName)))
//
//            case _ =>
//              throw NotImplementedException(s"Support for graph aliasing not yet implemented")
//          }
//        } yield result
//
//      case None =>
//        throw IllegalArgumentException("graph with alias", graph)
//    }
//  }

  private def convertReturnItem[R: _mayFail: _hasContext](item: ast.ReturnItem): Eff[R, (IRField, Expr)] = item match {

    case ast.AliasedReturnItem(e, v) =>
      for {
        expr <- convertExpr(e)
        context <- get[R, IRBuilderContext]
        field <- {
          val field = IRField(v.name)(expr.cypherType)
          put[R, IRBuilderContext](context.withFields(Set(field))) >> pure[R, IRField](field)
        }
      } yield field -> expr

    case ast.UnaliasedReturnItem(e, name) =>
      for {
        expr <- convertExpr(e)
        context <- get[R, IRBuilderContext]
        field <- {
          val field = IRField(name)(expr.cypherType)
          put[R, IRBuilderContext](context.withFields(Set(field))) >> pure[R, IRField](field)
        }
      } yield field -> expr

    case _ =>
      throw IllegalArgumentException(s"${AliasedReturnItem.getClass} or ${UnaliasedReturnItem.getClass}", item.getClass)
  }

  private def convertUnwindItem[R: _mayFail: _hasContext](
      list: Expression,
      variable: Variable): Eff[R, (Expr, IRField)] = {
    for {
      expr <- convertExpr(list)
      context <- get[R, IRBuilderContext]
      typ <- expr.cypherType.material match {
        case CTList(inner) =>
          pure[R, CypherType](inner)
        case CTAny =>
          pure[R, CypherType](CTAny)
        case x =>
          error(IRBuilderError(s"unwind expression was not a list: $x"))(CTWildcard: CypherType)
      }
      field <- {
        val field = IRField(variable.name)(typ)
        put[R, IRBuilderContext](context.withFields(Set(field))) >> pure[R, (Expr, IRField)](expr -> field)
      }
    } yield field
  }

  private def convertPattern[R: _hasContext](p: AstPattern): Eff[R, Pattern[Expr]] = {
    for {
      context <- get[R, IRBuilderContext]
      result <- {
        val pattern = context.convertPattern(p)
        val patternTypes = pattern.fields.foldLeft(context.knownTypes) {
          case (acc, f) => {
            acc.updated(Variable(f.name)(InputPosition.NONE), f.cypherType)
          }
        }
        put[R, IRBuilderContext](context.copy(knownTypes = patternTypes)) >> pure[R, Pattern[Expr]](pattern)
      }
    } yield result
  }

  private def convertExpr[R: _mayFail: _hasContext](e: Option[Expression]): Eff[R, Option[Expr]] =
    for {
      context <- get[R, IRBuilderContext]
    } yield
      e match {
        case Some(expr) => Some(context.convertExpression(expr))
        case None       => None
      }

  private def convertExpr[R: _mayFail: _hasContext](e: Expression): Eff[R, Expr] =
    for {
      context <- get[R, IRBuilderContext]
    } yield context.convertExpression(e)

  private def convertWhere[R: _mayFail: _hasContext](where: Option[ast.Where]): Eff[R, Set[Expr]] = where match {
    case Some(ast.Where(expr)) =>
      for {
        predicate <- convertExpr(expr)
      } yield {
        predicate match {
          case org.opencypher.okapi.ir.api.expr.Ands(exprs) => exprs
          case e                                           => Set(e)
        }
      }

    case None =>
      pure[R, Set[Expr]](Set.empty[Expr])
  }

  private def convertRegistry[R: _mayFail: _hasContext]: Eff[R, Option[CypherQuery[Expr]]] =
    for {
      context <- get[R, IRBuilderContext]
    } yield {
      val blocks = context.blocks
      val (ref, r) = blocks.reg.collectFirst {
        case (_ref, r: ResultBlock[Expr]) => _ref -> r
      }.get

      val model = QueryModel(r, context.parameters, blocks.reg.toMap - ref)
      val info = QueryInfo(context.queryString)

      Some(CypherQuery(info, model))
    }

  private def convertSortItem[R: _mayFail: _hasContext](item: ast.SortItem): Eff[R, SortItem[Expr]] = {
    item match {
      case ast.AscSortItem(astExpr) =>
        for {
          expr <- convertExpr(astExpr)
        } yield Asc(expr)
      case ast.DescSortItem(astExpr) =>
        for {
          expr <- convertExpr(astExpr)
        } yield Desc(expr)
    }
  }
}
