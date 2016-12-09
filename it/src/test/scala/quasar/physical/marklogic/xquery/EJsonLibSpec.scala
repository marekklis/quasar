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

package quasar.physical.marklogic.xquery

import quasar.Predef._
import quasar.{Data, TestConfig}
import quasar.physical.marklogic.ErrorMessages
import quasar.physical.marklogic.xcc._
import quasar.physical.marklogic.fs._
import quasar.physical.marklogic.xquery.syntax._

import com.marklogic.xcc.ContentSource
import scalaz._, Scalaz._

final class EJsonLibSpec extends quasar.Qspec {
  type M[A] = Writer[Prologs, A]

  def evaluateXQuery(xqy: M[XQuery], cs: ContentSource): ErrorMessages \/ Data = {
    val (prologs, body) = xqy.run

    val result = for {
      qr <- SessionIO.evaluateModule_(MainModule(Version.`1.0-ml`, prologs, body))
      rs <- SessionIO.liftT(qr.toImmutableArray)
      xi =  rs.headOption \/> "No results found.".wrapNel
    } yield xi >>= xdmitem.toData[ErrorMessages \/ ?] _

    (ContentSourceIO.runNT(cs) compose ContentSourceIO.runSessionIO)
      .apply(result)
      .unsafePerformSync
  }

  TestConfig.fileSystemConfigs(FsType).flatMap(_ traverse_ { case (backend, uri, _) =>
    contentSourceAt(uri).map { cs =>
      val eval: M[XQuery] => ErrorMessages \/ Data = evaluateXQuery(_, cs)
      s"XQuery EJSON Library (${backend.name})" >> {
        "many-to-array" >> {
          "passes through empty seq" >> {
            eval(ejson.manyToArray[M] apply expr.emptySeq).toOption must beNone
          }

          "passes through single item" >> {
            eval(ejson.manyToArray[M] apply "foo".xs) must_= Data._str("foo").right
          }

          "returns array for more than one item" >> {
            val three = mkSeq_("foo".xs, "bar".xs, "baz".xs)
            eval(ejson.manyToArray[M] apply three) must_= Data._arr(List(
              Data._str("foo"),
              Data._str("bar"),
              Data._str("baz")
            )).right
          }
        }
      }
    }.void
  }).unsafePerformSync
}
