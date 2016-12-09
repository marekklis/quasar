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

package quasar.ejson

import quasar.Predef.{Map => SMap, _}

import jawn.{Facade, SimpleFacade, SupportParser}
import matryoshka._
import scalaz.{:<:, Functor}

object jsonParser {
  def apply[T[_[_]]: Corecursive, F[_]: Functor](implicit C: Common :<: F, O: Obj :<: F): SupportParser[T[F]] =
    new SupportParser[T[F]] {
      implicit val facade: Facade[T[F]] =
        new SimpleFacade[T[F]] {
          def jarray(arr: List[T[F]])          = C(Arr(arr)).embed
          def jobject(obj: SMap[String, T[F]]) = O(Obj(ListMap(obj.toList: _*))).embed
          def jnull()                          = C(Null[T[F]]()).embed
          def jfalse()                         = C(Bool[T[F]](false)).embed
          def jtrue()                          = C(Bool[T[F]](true)).embed
          def jnum(n: String)                  = C(Dec[T[F]](BigDecimal(n))).embed
          def jint(n: String)                  = C(Dec[T[F]](BigDecimal(n))).embed
          def jstring(s: String)               = C(Str[T[F]](s)).embed
        }
    }
}
