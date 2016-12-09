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
import quasar.{Data, DataCodec, TestConfig}
import quasar.common.PhaseResultT
import quasar.contrib.matryoshka.{freeCataM, interpretM}
import quasar.effect.{MonotonicSeq, Read}
import quasar.fp.free._
import quasar.fp.reflNT
import quasar.fp.ski.κ
import quasar.fp.tree.{UnaryArg, BinaryArg, TernaryArg}
import quasar.fs.FileSystemError
import quasar.physical.couchbase.common.{CBDataCodec, Context}
import quasar.physical.couchbase.fs.{context, FsType}
import quasar.physical.couchbase.fs.queryfile.{n1qlResults, Plan}
import quasar.physical.couchbase.N1QL.{n1qlQueryString, partialQueryString}
import quasar.physical.couchbase.planner.CBPhaseLog
import quasar.physical.couchbase.planner.Planner.mapFuncPlanner
import quasar.Planner.{NonRepresentableData, PlannerError}
import quasar.qscript.{MapFunc, MapFuncStdLibTestRunner, FreeMapA}
import quasar.std.StdLibSpec

import matryoshka.Fix
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.specs2.execute.Result
import scalaz._, Scalaz._
import scalaz.concurrent.Task

class CouchbaseStdLibSpec extends StdLibSpec {
  implicit val codec = CBDataCodec

  type Eff0[A] = Coproduct[MonotonicSeq, Read[Context, ?], A]
  type Eff[A]  = Coproduct[Task, Eff0, A]

  type F[A] = Free[Eff, A]
  type M[A] = CBPhaseLog[F, A]

  def run[A](
    fm: Free[MapFunc[Fix, ?], A],
    args: A => Data,
    expected: Data,
    ctx: Context
  ): Result = {

    def argN1ql(d: Data): M[N1QL] =
      EitherT(DataCodec.render(d).bimap(
          κ(NonRepresentableData(d): PlannerError),
          partialQueryString(_)
      ).point[F].liftM[PhaseResultT])

    val q: M[N1QL] =
      freeCataM(fm)(interpretM(a => argN1ql(args(a)), mapFuncPlanner[F, Fix].plan))

    val r: FileSystemError \/ (String, Vector[Data]) =
      (
        q.leftMap(FileSystemError.qscriptPlanningFailed(_)) >>= { qq =>
          val qStr = s"select ${n1qlQueryString(qq)} v"
          (n1qlResults[Eff](partialQueryString(qStr)) ∘ (_ >>= {
            case Data.Obj(v) => v.values.toVector
            case v           => Vector(v)
          }): Plan[Eff, Vector[Data]]).strengthL(qStr)
        }
      ).run.run.foldMap(
        reflNT[Task]                            :+:
        MonotonicSeq.fromZero.unsafePerformSync :+:
        Read.constant[Task, Context](ctx)
      ).unsafePerformSync._2

      (r must beRightDisjunction.like { case (q, Vector(d)) =>
        d must beCloseTo(expected).updateMessage(_ ⊹ s"\nquery: $q")
      }).toResult
  }

  def runner(ctx: Context) = new MapFuncStdLibTestRunner {
    def nullaryMapFunc(
      prg: FreeMapA[Fix, Nothing],
      expected: Data
    ): Result =
      skipped

    def unaryMapFunc(
      prg: FreeMapA[Fix, UnaryArg],
      arg: Data,
      expected: Data
    ): Result =
      run(prg, κ(arg), expected, ctx)

    def binaryMapFunc(
      prg: FreeMapA[Fix, BinaryArg],
      arg1: Data, arg2: Data,
      expected: Data
    ): Result =
      run[BinaryArg](prg, _.fold(arg1, arg2), expected, ctx)

    def ternaryMapFunc(
      prg: FreeMapA[Fix, TernaryArg],
      arg1: Data, arg2: Data, arg3: Data,
      expected: Data
    ): Result =
      run[TernaryArg](prg, _.fold(arg1, arg2, arg3), expected, ctx)

    // TODO: remove let once '\\' is fixed in N1QL
    val genPrintableAsciiSansBackslash: Gen[String] =
      Gen.listOf(Gen.frequency(
        (64, Gen.choose('\u0020', '\u005B')),
        (36, Gen.choose('\u005D', '\u007e'))
      )).map(_.mkString)

    val intDomain: Gen[BigInt] = arbitrary[Int] map (BigInt(_))
    val decDomain: Gen[BigDecimal] = arbitrary[Double] map (BigDecimal(_))
    val stringDomain: Gen[String] = genPrintableAsciiSansBackslash
  }

  TestConfig.fileSystemConfigs(FsType).flatMap(_ traverse_ { case (backend, uri, _) =>
    context(uri).fold(
      err => Task.fail(new RuntimeException(err.shows)),
      ctx => Task.now(backend.name should tests(runner(ctx)))
    ).void
  }).unsafePerformSync

}
