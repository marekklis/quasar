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

package quasar.contrib

import quasar.ejson

import scala.collection.immutable.ListMap

import _root_.argonaut._, Argonaut._
import _root_.matryoshka._
import _root_.scalaz._

package object argonaut {
  implicit def jsonRecursive[J[_]]
    (implicit O: ejson.Obj :<: J, C: ejson.Common :<: J)
      : Recursive.Aux[Json, J] =
    new Recursive[Json] {
      type Base[A] = J[A]

      def project(t: Json)(implicit BF: Functor[Base]) =
        t.fold(
          C.inj(ejson.Null()),
          b => C.inj(ejson.Bool(b)),
          d => C.inj(ejson.Dec(d.toBigDecimal)),
          s => C.inj(ejson.Str(s)),
          a => C.inj(ejson.Arr(a)),
          o => O.inj(ejson.Obj(ListMap(o.toList: _*))))
    }

  // TODO: It would be nice to define this in terms of the components as well,
  //       but then I don’t know how to make it total. So, for now, we use the
  //       predefined Coproduct.
  implicit def jsonCorecursive: Corecursive.Aux[Json, ejson.Json] =
    new Corecursive[Json] {
      type Base[A] = ejson.Json[A]

      def embed(ft: ejson.Json[Json])(implicit BF: Functor[Base]) =
        ft match {
          case ejson.Common(ejson.Null())  => jNull
          case ejson.Common(ejson.Bool(b)) => jBool(b)
          case ejson.Common(ejson.Dec(d))  => jNumber(d)
          case ejson.Common(ejson.Str(s))  => jString(s)
          case ejson.Common(ejson.Arr(a))  => jArray(a)
          case ejson.Obj(ejson.Obj(o))     => jObject(JsonObject.fromTraversableOnce(o))
        }
    }
}
