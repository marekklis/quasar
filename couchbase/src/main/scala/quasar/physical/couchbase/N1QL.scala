/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.physical.couchbase

import quasar.Predef._
import quasar.DataCodec.Precise.NAKey

import monocle.macros.Lenses
import monocle.Prism
import scalaz._, Scalaz._

sealed trait N1QL

// TODO: Intentionally limited for the moment

object N1QL {

  final case class PartialQueryString(v: String) extends N1QL

  final case class Read(v: String) extends N1QL

  // TODO: N1QL instead of String
  @Lenses final case class Select(
    value: Boolean,
    resultExprs: NonEmptyList[String],
    keyspace: Option[N1QL],
    keyspaceAlias: Option[String],
    let: Option[Map[String, String]],
    filter: Option[String],
    groupBy: Option[String],
    unnest: Option[(String, String)],
    orderBy: Option[String]
  ) extends N1QL {
    def n1ql: N1QL = this
  }

  val partialQueryString = Prism.partial[N1QL, String] {
    case PartialQueryString(v) => v
  } (PartialQueryString(_))

  val read = Prism.partial[N1QL, String] {
    case Read(v) => v
  } (Read(_))

  def select(
    value: Boolean,
    resultExprs: NonEmptyList[String],
    keyspace: N1QL,
    keyspaceAlias: String
  ): Select =
    Select(value, resultExprs, keyspace.some, keyspaceAlias.some, none, none, none, none, none)

  def selectLet(
    value: Boolean,
    resultExprs: NonEmptyList[String],
    let: Map[String, String]
  ): Select =
    Select(value, resultExprs, none, none, let.some, none, none, none, none)

  val naStr = s"""{ "$NAKey": null }"""

  def selectN1qlQueryString(sel: Select): String = {
    val ks = sel.keyspace.map {
      case v: Select             => n1qlQueryString(v)
      case PartialQueryString(v) => s"(select value $v)"
      case Read(v)               => v
    }

    val value = sel.value.fold("value ", "")

    val resultExprs = sel.resultExprs.intercalate(", ")

    val ksAlias = sel.keyspaceAlias.cata(" as " + _, "")

    val let = sel.let.map(_.map { case (k, v) => s"$k = $v" }.mkString(" let ", ", ", ""))

    // TODO: Workaround for the moment. Lift "null" into a N1QL type?
    val groupBy = sel.groupBy.flatMap(g => (g ≠ "null").option(
      s" group by $g having $g is not null"))

    val unnest = sel.unnest.map(u => s" unnest ifnull(${u._1}, $naStr) ${u._2}")

    "("                                              |+|
    s"select $value$resultExprs"                     |+|
    ks         .map(k => s" from $k$ksAlias").orZero |+|
    sel.filter .map(f => s" where $f"       ).orZero |+|
    let                                      .orZero |+|
    groupBy                                  .orZero |+|
    unnest                                   .orZero |+|
    sel.orderBy.map(o => s" order by $o"    ).orZero |+|
    ")"
  }

  def n1qlQueryString(n1ql: N1QL): String =
    n1ql match {
      case s: Select             => selectN1qlQueryString(s)
      case PartialQueryString(v) => v
      case Read(v)               => v
    }

  def outerN1ql(n1ql: N1QL): N1QL = {
    val n1qlStr = n1qlQueryString(
      n1ql match {
        case PartialQueryString(v) => partialQueryString(s"(select value $v)")
        case v => v
      })

    partialQueryString(s"select value v from $n1qlStr as v")
  }

  implicit val show: Show[N1QL] = Show.showFromToString

}
