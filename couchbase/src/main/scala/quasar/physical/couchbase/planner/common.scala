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

package quasar.physical.couchbase.planner

import quasar.Predef._
import quasar.connector.PlannerErrT
import quasar.contrib.pathy._
import quasar.physical.couchbase, couchbase._
import quasar.physical.couchbase.N1QL.{Read => _, _}
import quasar.Planner.PlannerError
import quasar.qscript.{IdStatus, IdOnly, IncludeId, ExcludeId}

import scalaz._, Scalaz._

object common {
  def readPath[F[_]: Applicative](path: APath, idStatus: IdStatus): PlannerErrT[F, N1QL] =
    EitherT(
      couchbase.common.BucketCollection.fromPath(path).bimap[PlannerError, N1QL](
        quasar.Planner.PlanPathError(_),
        bc => {
          val v = "ifmissing(v.`value`, v)"
          val r = idStatus match {
            case IdOnly    => "meta(v).id"
            case IncludeId => "[meta(v).id, ifmissing(v.`value`, v)]"
            case ExcludeId => v
          }
          val f = bc.collection.isEmpty.fold("", s""" where type = "${bc.collection}"""")
          // TODO: Special select case for the moment
          read(s"(select value $r from `${bc.bucket}` v$f)")
        }
      ).point[F])
}
