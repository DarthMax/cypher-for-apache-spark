package org.opencypher.spark.testing

import java.io.{File, PrintWriter}

import org.opencypher.okapi.impl.exception.NotImplementedException
import org.opencypher.okapi.tck.test.ScenariosFor
import org.opencypher.tools.tck.api._
import org.scalatest.prop.TableFor1
import org.apache.commons.lang.StringEscapeUtils
import org.opencypher.tools.tck.values.CypherValue


object AcceptanceTestGenerator extends App {
  private val failingBlacklist = getClass.getResource("/failing_blacklist").getFile
  private val temporalBlacklist = getClass.getResource("/temporal_blacklist").getFile
  private val wontFixBlacklistFile = getClass.getResource("/wont_fix_blacklist").getFile
  private val failureReportingBlacklistFile = getClass.getResource("/failure_reporting_blacklist").getFile
  private val scenarios: ScenariosFor = ScenariosFor(failingBlacklist, temporalBlacklist, wontFixBlacklistFile, failureReportingBlacklistFile)
  private val lineIndention = "\t\t"


  private def generateClassFiles(featureName: String, scenarios: TableFor1[Scenario], black: Boolean) = {
    val path = s"spark-cypher-testing/src/test/scala/org/opencypher/spark/impl/acceptance/"
    val packageName = if (black) "blackList" else "whiteList"
    val className = s"${featureName}_$packageName"
    val classHeader =
      s"""|package org.opencypher.spark.impl.acceptance.$packageName
          |
          |import org.scalatest.junit.JUnitRunner
          |import org.junit.runner.RunWith
          |import org.opencypher.okapi.tck.test.CypherToTCKConverter
          |import org.opencypher.spark.testing.CAPSTestSuite
          |import org.opencypher.spark.impl.acceptance.ScanGraphInit
          |import org.apache.commons.lang.StringEscapeUtils
          |import org.opencypher.spark.impl.graph.CAPSGraphFactory
          |import scala.util.{Failure, Success, Try}
          |
          |@RunWith(classOf[JUnitRunner])
          |class $className extends CAPSTestSuite with ScanGraphInit {""".stripMargin


    val testCases = "\n" + scenarios.map(scenario =>
      if (scenario.name.equals("Failing on incorrect unicode literal")) "" //this fails at compilation
      else
        generateTest(scenario, black)).mkString("\n")

    val file = new File(s"$path/$packageName/$className.scala")
    val fileString =
      s"""$classHeader
         |$testCases
         |}""".stripMargin
    val out = new PrintWriter(file)
    out.print(fileString)
    out.close()
    file.createNewFile()
  }

  private def stringEscape(s : String):String = {
    s.replace("\n","\\n").replace("\t","\\t")
  }

  //result consists of (step-type, step-string)
  private def stepToStringTuple(step: Step): (String, String) = {
    step match {
      case Execute(query, querytype, _) =>
        val alignedQuery = query.replace("\n", s"\n$lineIndention\t")
        querytype match {
          case InitQuery => "init" -> alignedQuery
          case ExecQuery => "exec" -> alignedQuery
          case SideEffectQuery =>
            //currently no TCK-Tests with side effect queries
            throw NotImplementedException("Side Effect Queries not supported yet")
        }
      case ExpectResult(expectedResult: CypherValueRecords, _, sorted) =>
        //result -> expected
        val resultRows = if (sorted)
          "resultValueRecords.rows" -> expectedResult.rows
        else
          "resultValueRecords.rows.sortBy(_.hashCode())" -> expectedResult.rows.sortBy(_.hashCode())

        "result" ->
          s"""
             |${lineIndention}val resultValueRecords = CypherToTCKConverter.convertToTckStrings(result.records).asValueRecords
             |${lineIndention}StringEscapeUtils.escapeJava(resultValueRecords.header.toString()) should equal("${StringEscapeUtils.escapeJava(stringEscape(expectedResult.header.toString))}")
             |${lineIndention}StringEscapeUtils.escapeJava(${resultRows._1}.toString()) should equal("${StringEscapeUtils.escapeJava(stringEscape(resultRows._2.toString))}")
           """.stripMargin
      case ExpectError(errorType, errorPhase, detail, _) =>
        "error" -> errorType //todo: also return detail here
      case SideEffects(expected, _) =>
        val relevantEffects = expected.v.filter(_._2 > 0) //check if relevant Side-Effects exist
        if (relevantEffects.nonEmpty)
          "sideeffect" -> s"fail() //TODO: handle side effects"
        /*Todo: calculate via before and after State? (can result graph return nodes/relationships/properties/labels as a set of cyphervalues?)
        todo: maybe also possible via Cypher-Queries (may take too long?) */
        else
          "" -> ""
      case Parameters(v,_) => "param" -> v.foldLeft(""){(acc,x) =>
        val value: CypherValue = x._2 //Todo: write unwrap for CypherValue?
        acc + s"val ${x._1} = ${x._2} \n"}
      case _ => "" -> ""
    }
  }


  private def generateTest(scenario: Scenario, black: Boolean): String = {

    val escapeStringMarks = "\"\"\""
    val steps = scenario.steps.map {
      stepToStringTuple
    }.filter(_._2.nonEmpty)

    val initSteps = scenario.steps.filter{ case Execute(_, InitQuery, _) => true
        case _=> false }
    val initQuery = escapeStringMarks + initSteps.foldLeft("")((combined, x) => combined + s"\n$lineIndention\t" + stepToStringTuple(x)._2) + escapeStringMarks


    val executionQuerySteps = steps.filter(_._1.equals("exec")).map(_._2) //handle control query?
    val expectedResultSteps = steps.filter(_._1.startsWith("result")).map(_._2)
    val expectedErrorSteps = steps.filter(_._1.equals("error")).map(_._2) //only one error can be expected
    val sideEffectSteps = steps.filter(_._1.eq("sideeffect")).map(_._2)

    //todo allow f.i. expected error only for 2nd executionQuery
    val tests = executionQuerySteps.zipWithIndex.zipAll(expectedResultSteps, "", "").zipAll(expectedErrorSteps, "", "").zipAll(sideEffectSteps, "", "")
      .map { case ((((v, w), x), y), z) => (v, w, x, y, z) }

    val testResultStrings = tests.map {
      case (exec: String, num: Integer, result: String, expectedError: String, sideEffect: String) =>
        //todo: !! how to check for expectedErrorType?
        val errorString = if(expectedError.nonEmpty)
          s"""
             |    val result$num = an[Exception] shouldBe thrownBy{graph.cypher($escapeStringMarks$exec$escapeStringMarks)}
             |
           """.stripMargin
        else
          s"val result$num = graph.cypher($escapeStringMarks$exec$escapeStringMarks)"
        //todo: improve replacement as some are wrong
        s"""
           |    $errorString
           |    ${result.replace("result", s"result$num")}
           |    ${if (sideEffect.nonEmpty) "fail() //todo: check side effects" else ""}
        """.stripMargin
    }

    val testString =
      s"""
         |    val graph = ${if (initSteps.nonEmpty) s"initGraph($initQuery)" else "CAPSGraphFactory.apply().empty"}
         |    ${testResultStrings.mkString("\n        ")}
       """.stripMargin

   if (black)
      s"""  it("${scenario.name}") {
         |      Try({
         |        $testString
         |      }) match{
         |        case Success(_) =>
         |          throw new RuntimeException(s"A blacklisted scenario actually worked")
         |        case Failure(_) =>
         |          ()
         |      }
         |    }
      """.stripMargin
    else
      s"""  it("${scenario.name}") {
         |    $testString
         |  }
      """.stripMargin
  }

  //todo: clear directories before first write?
  val blackFeatures = scenarios.blackList.groupBy(_.featureName)
  val whiteFeatures = scenarios.whiteList.groupBy(_.featureName)

  whiteFeatures.map { feature => {
    generateClassFiles(feature._1, feature._2, black = false)
  }
  }

  blackFeatures.map { feature => {
    generateClassFiles(feature._1, feature._2, black = true)
  }
  }

}
