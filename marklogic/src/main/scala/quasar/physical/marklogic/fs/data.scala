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

package quasar.physical.marklogic.fs

import quasar.Predef._
import quasar.Data
import quasar.physical.marklogic.{ErrorMessages, MonadErrMsgs}
import quasar.physical.marklogic.prisms._
import quasar.physical.marklogic.xml._
import quasar.physical.marklogic.xml.namespaces._

import scala.xml._
import scala.collection.immutable.Seq

import eu.timepit.refined.auto._
import monocle.Prism
import scalaz.{Node => _, _}, Scalaz._

object data {
  def encodeXml[F[_]: MonadErrMsgs](data: Data): F[Elem] = {
    def typeAttr(tpe: DataType): Attribute =
      Attribute(ejsBinding.prefix, ejsonType.local.shows, tpe.shows, Null)

    def ejsElem(name: QName, tpe: Option[DataType], ns: NamespaceBinding, children: Seq[Node]): Elem =
      Elem(name.prefix.map(_.shows).orNull, name.local.shows, tpe.cata(typeAttr, Null), ns, true, children: _*)

    def innerElem(name: QName, tpe: DataType, children: Seq[Node]): Elem =
      ejsElem(name, Some(tpe), TopScope, children)

    def innerStr(name: QName, str: String): Elem =
      ejsElem(name, None, TopScope, Text(str))

    def rootElem(name: QName, tpe: DataType, children: Seq[Node]): Elem =
      ejsElem(name, Some(tpe), ejsBinding, children)

    def rootStr(name: QName, str: String): Elem =
      ejsElem(name, None, ejsBinding, Text(str))

    def encodeXml0(
      str: (QName, String) => Elem,
      elem: (QName, DataType, Seq[Node]) => Elem,
      loop: QName => Data => Validation[ErrorMessages, Elem]
    ): QName => Data => Validation[ErrorMessages, Elem] = {
      val mapEntryToXml: ((String, Data)) => ErrorMessages \/ Elem = {
        case (k, v) => for {
          nc <- NCName(k) leftAs s"'$k' is not a valid XML QName.".wrapNel
          el <- loop(QName.local(nc))(v).disjunction
        } yield el
      }

      elementName => {
        case Data.Binary(bytes) => elem(elementName, DT.Binary   , Text(base64Bytes(bytes))     ).success
        case Data.Bool(b)       => elem(elementName, DT.Boolean  , Text(b.fold("true", "false"))).success
        case Data.Date(d)       => elem(elementName, DT.Date     , Text(isoLocalDate(d))        ).success
        case Data.Dec(d)        => elem(elementName, DT.Decimal  , Text(d.toString)             ).success
        case Data.Id(id)        => elem(elementName, DT.Id       , Text(id)                     ).success
        case Data.Int(i)        => elem(elementName, DT.Integer  , Text(i.toString)             ).success
        case Data.Interval(d)   => elem(elementName, DT.Interval , Text(durationInSeconds(d))   ).success
        case Data.NA            => elem(elementName, DT.NA       , Nil                          ).success
        case Data.Null          => elem(elementName, DT.Null     , Nil                          ).success
        case Data.Str(s)        => str(elementName               , s                            ).success
        case Data.Time(t)       => elem(elementName, DT.Time     , Text(isoLocalTime(t))        ).success
        case Data.Timestamp(ts) => elem(elementName, DT.Timestamp, Text(isoInstant(ts))         ).success

        case Data.Arr(elements) =>
          elements traverse loop(ejsonArrayElt) map (elem(elementName, DT.Array, _))

        case Data.Obj(entries)  =>
          entries.toList traverse (mapEntryToXml andThen (_.validation)) map (elem(elementName, DT.Object, _))

        case other              => s"No representation for '$other' in XML.".failureNel[Elem]
      }
    }

    def inner: QName => Data => Validation[ErrorMessages, Elem] =
      name => encodeXml0(innerStr, innerElem, inner)(name)

    encodeXml0(rootStr, rootElem, inner)(ejsonEjson)(data)
      .fold(_.raiseError[F, Elem], _.point[F])
  }

  def decodeXml[F[_]: MonadErrMsgs](elem: Elem): F[Data] = {
    def decodeXml0: Node => Validation[ErrorMessages, Data] = {
      case DataNode(DT.Array, children) =>
        elements(children).toList traverse decodeXml0 map (Data._arr(_))

      case DataNode(DT.Binary, Txt(b64)) =>
        base64Bytes.getOption(b64)
          .map(bytes => Data._binary(bytes))
          .toSuccessNel(s"Expected Base64-encoded binary data, found: $b64")

      case DataNode(DT.Binary, Seq()   ) =>
        Data._binary(ImmutableArray.fromArray(Array())).success

      case DataNode(DT.Boolean, Txt("true"))  => Data._bool(true).success
      case DataNode(DT.Boolean, Txt("false")) => Data._bool(false).success

      case DataNode(DT.Date, Txt(d)) =>
        isoLocalDate.getOption(d)
          .map(Data._date(_))
          .toSuccessNel(s"Expected an ISO-8601 formatted local date, found: $d")

      case DataNode(DT.Decimal, Txt(d)) =>
        Validation.fromTryCatchNonFatal(BigDecimal(d))
          .map(Data._dec(_))
          .leftAs(s"Expected a decimal number, found: $d".wrapNel)

      case DataNode(DT.Id, Txt(id)) => Data._id(id).success
      case DataNode(DT.Id, Seq()  ) => Data._id("").success

      case DataNode(DT.Integer, Txt(n)) =>
        Validation.fromTryCatchNonFatal(BigInt(n))
          .map(Data._int(_))
          .leftAs(s"Expected an integral number, found: $n".wrapNel)

      case DataNode(DT.Interval, Txt(ivl)) =>
        durationInSeconds.getOption(ivl)
          .map(Data._interval(_))
          .toSuccessNel(s"Expected a duration in seconds, found: $ivl")

      case DataNode(DT.NA, Seq()) => (Data.NA: Data).success

      case DataNode(DT.Null, Seq()) => (Data.Null: Data).success

      case DataNode(DT.Object, children) =>
        elements(children).toList
          .traverse(el => decodeXml0(el) strengthL qualifiedName(el))
          .map(entries => Data._obj(ListMap(entries: _*)))

      case DataNode(DT.String, Txt(s)) => Data._str(s).success
      case DataNode(DT.String, Seq() ) => Data._str("").success

      case DataNode(DT.Time, Txt(t)) =>
        isoLocalTime.getOption(t)
          .map(Data._time(_))
          .toSuccessNel(s"Expected an ISO-8601 formatted local time, found: $t")

      case DataNode(DT.Timestamp, Txt(ts)) =>
        isoInstant.getOption(ts)
          .map(Data._timestamp(_))
          .toSuccessNel(s"Expected an ISO-8601 formatted date-time, found: $ts")

      case other => s"Unrecognized Data XML: $other.".failureNel
    }

    decodeXml0(elem).fold(_.raiseError[F, Data], _.point[F])
  }

  ////

  private sealed abstract class DataType {
    override def toString = Show[DataType].shows(this)
  }

  private object DataType {
    case object Array     extends DataType
    case object Binary    extends DataType
    case object Boolean   extends DataType
    case object Date      extends DataType
    case object Decimal   extends DataType
    case object Id        extends DataType
    case object Integer   extends DataType
    case object Interval  extends DataType
    case object NA        extends DataType
    case object Null      extends DataType
    case object Object    extends DataType
    case object String    extends DataType
    case object Time      extends DataType
    case object Timestamp extends DataType

    val stringCodec = Prism.partial[String, DataType] {
      case "array"     => Array
      case "binary"    => Binary
      case "boolean"   => Boolean
      case "date"      => Date
      case "decimal"   => Decimal
      case "id"        => Id
      case "integer"   => Integer
      case "interval"  => Interval
      case "na"        => NA
      case "null"      => Null
      case "object"    => Object
      case "string"    => String
      case "time"      => Time
      case "timestamp" => Timestamp
    } {
      case Array       => "array"
      case Binary      => "binary"
      case Boolean     => "boolean"
      case Date        => "date"
      case Decimal     => "decimal"
      case Id          => "id"
      case Integer     => "integer"
      case Interval    => "interval"
      case NA          => "na"
      case Null        => "null"
      case Object      => "object"
      case String      => "string"
      case Time        => "time"
      case Timestamp   => "timestamp"
    }

    implicit val show: Show[DataType] =
      Show.shows(stringCodec(_))
  }

  private val DT = DataType

  private object DataNode {
    def unapply(node: Node): Option[(DataType, Seq[Node])] = {
      val tpe = node.attributes collectFirst {
        case PrefixedAttribute(p, n, Seq(Text(t)), _) if s"$p:$n" === ejsonType.shows => t
      } getOrElse "string"

      DataType.stringCodec.getOption(tpe) strengthR node.child
    }
  }

  private object Txt {
    def unapply(nodes: Seq[Node]): Option[String] =
      nodes.headOption collect { case Text(s) => s }
  }

  private val ejsBinding: NamespaceBinding =
    NamespaceBinding(ejsonNs.prefix.shows, ejsonNs.uri.shows, TopScope)
}
