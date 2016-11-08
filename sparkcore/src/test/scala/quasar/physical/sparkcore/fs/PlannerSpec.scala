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

package quasar.physical.sparkcore.fs

import quasar.Predef._
import quasar.console
import quasar.qscript.QScriptHelpers
import quasar.qscript._
import quasar.qscript.ReduceFuncs._
import quasar.qscript.MapFuncs._
import quasar.contrib.pathy._
import quasar.Data
import quasar.DataCodec
import quasar.qscript._
import quasar.sql.JoinDir

import org.apache.spark._
import org.apache.spark.rdd._
import pathy.Path._
import scalaz._, Scalaz._, scalaz.concurrent.Task
import pathy.Path._
import matryoshka.{Hole => _, _}
import org.specs2.scalaz.DisjunctionMatchers

class PlannerSpec extends quasar.Qspec with QScriptHelpers with DisjunctionMatchers {

  import Planner.SparkState

  sequential

  val equi = Planner.equiJoin[Fix]
  val sr = Planner.shiftedread[Fix]
  val qscore = Planner.qscriptCore[Fix]

  "Planner" should {
    "shiftedread" in {

      newSc.map ( sc => {
        val fromFile: (SparkContext, AFile) => Task[RDD[String]] =
          (sc: SparkContext, file: AFile) => Task.delay {
            sc.parallelize(List("""{"name" : "tom", "age" : 28}"""))
          }
        val alg: AlgebraM[SparkState, Const[ShiftedRead, ?], RDD[Data]] = sr.plan(fromFile )
        val afile: AFile = rootDir </> dir("Users") </> dir("rabbit") </> file("test.json")

        val state: SparkState[RDD[Data]] = alg(Const(ShiftedRead(afile, ExcludeId)))
        state.eval(sc).run.unsafePerformSync must beRightDisjunction.like{
          case rdd =>
            val results = rdd.collect
            results.size must_== 1
            results(0) must_== Data.Obj(ListMap(
              "name" -> Data.Str("tom"),
              "age" -> Data.Int(28)
            ))
        }
        sc.stop
      }).run.unsafePerformSync
      ok
    }

    "core.map" in {
      newSc.map ( sc => {
        val alg: AlgebraM[SparkState, QScriptCore, RDD[Data]] = qscore.plan(emptyFF)

        val src: RDD[Data] = sc.parallelize(List(
          Data.Obj(ListMap() + ("age" -> Data.Int(24)) + ("country" -> Data.Str("Poland"))),
          Data.Obj(ListMap() + ("age" -> Data.Int(32)) + ("country" -> Data.Str("Poland"))),
          Data.Obj(ListMap() + ("age" -> Data.Int(23)) + ("country" -> Data.Str("US")))
        ))

        def func: FreeMap = ProjectFieldR(HoleF, StrLit("country"))
        val map = quasar.qscript.Map(src, func)

        val state: SparkState[RDD[Data]] = alg(map)
        state.eval(sc).run.unsafePerformSync  must beRightDisjunction.like{
          case rdd =>
            val results = rdd.collect
            results.size must_= 3
            results(0) must_= Data.Str("Poland")
            results(1) must_= Data.Str("Poland")
            results(2) must_= Data.Str("US")
        }
        sc.stop
      }).run.unsafePerformSync
      ok
    }

    "core.reduce" in {
      newSc.map ( sc => {
        val alg: AlgebraM[SparkState, QScriptCore, RDD[Data]] = qscore.plan(emptyFF)

        val src: RDD[Data] = sc.parallelize(List(
          Data.Obj(ListMap() + ("age" -> Data.Int(24)) + ("country" -> Data.Str("Poland"))),
          Data.Obj(ListMap() + ("age" -> Data.Int(32)) + ("country" -> Data.Str("Poland"))),
          Data.Obj(ListMap() + ("age" -> Data.Int(23)) + ("country" -> Data.Str("US")))
        ))

        def bucket: FreeMap = ProjectFieldR(HoleF, StrLit("country"))
        def reducers: List[ReduceFunc[FreeMap]] = List(Arbitrary(ProjectFieldR(HoleF, StrLit("country"))))
        def repair: Free[MapFunc, ReduceIndex] = Free.point(ReduceIndex(0))
        val reduce = Reduce(src, bucket, reducers, repair)

        val state: SparkState[RDD[Data]] = alg(reduce)
        state.eval(sc).run.unsafePerformSync  must beRightDisjunction.like{
          case rdd =>
            val results = rdd.collect
            results.size must_= 2
            results(1) must_= Data.Str("Poland")
            results(0) must_= Data.Str("US")
        }
        sc.stop
      }).run.unsafePerformSync
      ok
    }

    "core.reduce.max" in {
      newSc.map ( sc => {
        val alg: AlgebraM[SparkState, QScriptCore, RDD[Data]] = qscore.plan(emptyFF)

        val src: RDD[Data] = sc.parallelize(List(
          Data.Obj(ListMap() + ("age" -> Data.Int(24)) + ("country" -> Data.Str("Poland"))),
          Data.Obj(ListMap() + ("age" -> Data.Int(32)) + ("country" -> Data.Str("Poland"))),
          Data.Obj(ListMap() + ("age" -> Data.Int(23)) + ("country" -> Data.Str("US")))
        ))

        def bucket: FreeMap = ProjectFieldR(HoleF, StrLit("country"))
        def reducers: List[ReduceFunc[FreeMap]] = List(Max(ProjectFieldR(HoleF, StrLit("age"))))
        def repair: Free[MapFunc, ReduceIndex] = Free.point(ReduceIndex(0))
        val reduce = Reduce(src, bucket, reducers, repair)

        val state: SparkState[RDD[Data]] = alg(reduce)
        state.eval(sc).run.unsafePerformSync  must beRightDisjunction.like{
          case rdd =>
            val results = rdd.collect
            results.size must_= 2
            results(1) must_= Data.Int(32)
            results(0) must_= Data.Int(23)
        }
        sc.stop
      }).run.unsafePerformSync
      ok
    }

    "core.sort" in {
      newSc.map ( sc => {
        val alg: AlgebraM[SparkState, QScriptCore, RDD[Data]] = qscore.plan(emptyFF)

        val src: RDD[Data] = sc.parallelize(List(
          Data.Obj(ListMap() + ("age" -> Data.Int(24)) + ("country" -> Data.Str("Poland"))),
          Data.Obj(ListMap() + ("age" -> Data.Int(32)) + ("country" -> Data.Str("Austria"))),
          Data.Obj(ListMap() + ("age" -> Data.Int(23)) + ("country" -> Data.Str("US")))
        ))

        def bucket = ProjectFieldR(HoleF, StrLit("country"))
        def order = List((bucket, SortDir.Ascending))
        val sort = quasar.qscript.Sort(src, bucket, order)

        val state: SparkState[RDD[Data]] = alg(sort)
        state.eval(sc).run.unsafePerformSync  must beRightDisjunction.like{
          case rdd =>
            val results = rdd.collect
            results must_== Array(
              Data.Obj(ListMap("age" -> Data.Int(32), "country" -> Data.Str("Austria"))),
              Data.Obj(ListMap("age" -> Data.Int(24), "country" -> Data.Str("Poland"))),
              Data.Obj(ListMap("age" -> Data.Int(23), "country" -> Data.Str("US")))
            )
        }
        sc.stop
      }).run.unsafePerformSync
      ok
    }

    "core.filter" in {
      newSc.map ( sc => {
        val alg: AlgebraM[SparkState, QScriptCore, RDD[Data]] = qscore.plan(emptyFF)

        val src: RDD[Data] = sc.parallelize(List(
          Data.Obj(ListMap() + ("age" -> Data.Int(24)) + ("country" -> Data.Str("Poland"))),
          Data.Obj(ListMap() + ("age" -> Data.Int(32)) + ("country" -> Data.Str("Poland"))),
          Data.Obj(ListMap() + ("age" -> Data.Int(23)) + ("country" -> Data.Str("US")))
        ))

        def func: FreeMap = Free.roll(Lt(ProjectFieldR(HoleF, StrLit("age")), IntLit(24)))
        val filter = quasar.qscript.Filter(src, func)

        val state: SparkState[RDD[Data]] = alg(filter)
        state.eval(sc).run.unsafePerformSync  must beRightDisjunction.like{
          case rdd =>
            val results = rdd.collect
            results.size must_= 1
            results(0) must_= Data.Obj(ListMap(
              "age" -> Data.Int(23),
              "country" -> Data.Str("US")
            ))
        }
        sc.stop
      }).run.unsafePerformSync
      ok
    }

    "core.take" in {
      newSc.map ( sc => {
        val alg: AlgebraM[SparkState, QScriptCore, RDD[Data]] = qscore.plan(emptyFF)

        val src: RDD[Data] = sc.parallelize(List(
          Data.Obj(ListMap() + ("age" -> Data.Int(24)) + ("country" -> Data.Str("Poland"))),
          Data.Obj(ListMap() + ("age" -> Data.Int(32)) + ("country" -> Data.Str("Poland"))),
          Data.Obj(ListMap() + ("age" -> Data.Int(23)) + ("country" -> Data.Str("US"))),
          Data.Obj(ListMap() + ("age" -> Data.Int(32)) + ("country" -> Data.Str("US")))
        ))

        def from: FreeQS = Free.point(SrcHole)
        def count: FreeQS = constFreeQS(1)

        val take = quasar.qscript.Subset(src, from, Take, count)

        val state: SparkState[RDD[Data]] = alg(take)
        state.eval(sc).run.unsafePerformSync  must beRightDisjunction.like{
          case rdd =>
            val results = rdd.collect
            results.size must_= 1
            results(0) must_= Data.Obj(ListMap(
              "age" -> Data.Int(24),
              "country" -> Data.Str("Poland")
            ))
        }
        sc.stop
      }).run.unsafePerformSync
      ok
    }

    "core.drop" in {
      newSc.map ( sc => {
        val alg: AlgebraM[SparkState, QScriptCore, RDD[Data]] = qscore.plan(emptyFF)

        val src: RDD[Data] = sc.parallelize(List(
          Data.Obj(ListMap() + ("age" -> Data.Int(24)) + ("country" -> Data.Str("Poland"))),
          Data.Obj(ListMap() + ("age" -> Data.Int(32)) + ("country" -> Data.Str("Poland"))),
          Data.Obj(ListMap() + ("age" -> Data.Int(23)) + ("country" -> Data.Str("US"))),
          Data.Obj(ListMap() + ("age" -> Data.Int(32)) + ("country" -> Data.Str("US")))
        ))

        def from: FreeQS = Free.point(SrcHole)
        def count: FreeQS = constFreeQS(3)

        val drop = quasar.qscript.Subset(src, from, Drop, count)

        val state: SparkState[RDD[Data]] = alg(drop)
        state.eval(sc).run.unsafePerformSync  must beRightDisjunction.like{
          case rdd =>
            val results = rdd.collect
            results.size must_= 1
            results(0) must_= Data.Obj(ListMap(
              "age" -> Data.Int(32),
              "country" -> Data.Str("US")
            ))
        }
        sc.stop
      }).run.unsafePerformSync
      ok
    }

    "core.union" in {
      newSc.map ( sc => {
        val alg: AlgebraM[SparkState, QScriptCore, RDD[Data]] = qscore.plan(emptyFF)

        val src: RDD[Data] = sc.parallelize(List(
          Data.Obj(ListMap() + ("age" -> Data.Int(24)) + ("country" -> Data.Str("Poland"))),
          Data.Obj(ListMap() + ("age" -> Data.Int(32)) + ("country" -> Data.Str("Poland"))),
          Data.Obj(ListMap() + ("age" -> Data.Int(23)) + ("country" -> Data.Str("US"))),
          Data.Obj(ListMap() + ("age" -> Data.Int(14)) + ("country" -> Data.Str("UK")))
        ))

        def func(country: String): FreeMap =
          Free.roll(MapFuncs.Eq(ProjectFieldR(HoleF, StrLit("country")), StrLit(country)))

        def left: FreeQS = Free.roll(QCT.inj(Filter(HoleQS, func("Poland"))))
        def right: FreeQS = Free.roll(QCT.inj(Filter(HoleQS, func("US"))))

        val union = quasar.qscript.Union(src, left, right)

        val state: SparkState[RDD[Data]] = alg(union)
        state.eval(sc).run.unsafePerformSync  must beRightDisjunction.like{
          case rdd =>
            rdd.collect.toList must_= List(
              Data.Obj(ListMap() + ("age" -> Data.Int(24)) + ("country" -> Data.Str("Poland"))),
              Data.Obj(ListMap() + ("age" -> Data.Int(32)) + ("country" -> Data.Str("Poland"))),
              Data.Obj(ListMap() + ("age" -> Data.Int(23)) + ("country" -> Data.Str("US")))
            )
        }
        sc.stop
      }).run.unsafePerformSync
      ok
    }

    "core.leftshift" in {
      newSc.map ( sc => {
        val alg: AlgebraM[SparkState, QScriptCore, RDD[Data]] = qscore.plan(emptyFF)

        val src: RDD[Data] = sc.parallelize(List(
          Data.Obj(ListMap(("age" -> Data.Int(24)),("countries" -> Data.Arr(List(Data.Str("Poland"), Data.Str("US")))))),
          Data.Obj(ListMap(("age" -> Data.Int(24)),("countries" -> Data.Arr(List(Data.Str("UK"))))))
        ))

        def struct: FreeMap = ProjectFieldR(HoleF, StrLit("countries"))
        def repair: JoinFunc = Free.point(RightSide)

        val leftShift = quasar.qscript.LeftShift(src, struct, repair)

        val state: SparkState[RDD[Data]] = alg(leftShift)
        state.eval(sc).run.unsafePerformSync  must beRightDisjunction.like{
          case rdd =>
            rdd.collect.toList must_= List(
              Data.Str("Poland"),
              Data.Str("US"),
              Data.Str("UK")
            )
        }

        sc.stop
      }).run.unsafePerformSync
      ok
    }

    "equiJoin.inner" in {

      newSc.map ( sc => {
        val alg: AlgebraM[SparkState, EquiJoin, RDD[Data]] = equi.plan(emptyFF)

        val src: RDD[Data] = sc.parallelize(List(
          Data.Obj(ListMap(("age" -> Data.Int(24)), ("country" -> Data.Str("Poland")))),
          Data.Obj(ListMap(("age" -> Data.Int(32)), ("country" -> Data.Str("Poland")))),
          Data.Obj(ListMap(("age" -> Data.Int(24)), ("country" -> Data.Str("US")))),
          Data.Obj(ListMap(("age" -> Data.Int(14)), ("country" -> Data.Str("UK"))))
        ))

        def func(country: String): FreeMap =
          Free.roll(MapFuncs.Eq(ProjectFieldR(HoleF, StrLit("country")), StrLit(country)))

        def left: FreeQS = Free.roll(QCT.inj(Filter(HoleQS, func("Poland"))))
        def right: FreeQS = Free.roll(QCT.inj(Filter(HoleQS, func("US"))))
        def key: FreeMap = ProjectFieldR(HoleF, StrLit("age"))
        def combine: JoinFunc = Free.roll(ConcatMaps(
          Free.roll(MakeMap(StrLit(JoinDir.Left.name), LeftSideF)),
          Free.roll(MakeMap(StrLit(JoinDir.Right.name), RightSideF))
        ))

        val equiJoin = quasar.qscript.EquiJoin(src, left, right, key, key, Inner, combine)

        val state: SparkState[RDD[Data]] = alg(equiJoin)
        state.eval(sc).run.unsafePerformSync  must beRightDisjunction.like{
          case rdd =>
            rdd.collect.toList must_= List(
              Data.Obj(ListMap(
                JoinDir.Left.name -> Data.Obj(ListMap(("age" -> Data.Int(24)), ("country" -> Data.Str("Poland")))),
                JoinDir.Right.name -> Data.Obj(ListMap(("age" -> Data.Int(24)), ("country" -> Data.Str("US"))))
              )
            ))
        }
        sc.stop
      }).run.unsafePerformSync
      ok
    }

    "equiJoin.leftOuter" in {

      newSc.map ( sc => {
        val alg: AlgebraM[SparkState, EquiJoin, RDD[Data]] = equi.plan(emptyFF)

        val src: RDD[Data] = sc.parallelize(List(
          Data.Obj(ListMap(("age" -> Data.Int(24)), ("country" -> Data.Str("Poland")))),
          Data.Obj(ListMap(("age" -> Data.Int(32)), ("country" -> Data.Str("Poland")))),
          Data.Obj(ListMap(("age" -> Data.Int(24)), ("country" -> Data.Str("US")))),
          Data.Obj(ListMap(("age" -> Data.Int(14)), ("country" -> Data.Str("UK"))))
        ))

        def func(country: String): FreeMap =
          Free.roll(MapFuncs.Eq(ProjectFieldR(HoleF, StrLit("country")), StrLit(country)))

        def left: FreeQS = Free.roll(QCT.inj(Filter(HoleQS, func("Poland"))))
        def right: FreeQS = Free.roll(QCT.inj(Filter(HoleQS, func("US"))))
        def key: FreeMap = ProjectFieldR(HoleF, StrLit("age"))
        def combine: JoinFunc = Free.roll(ConcatMaps(
          Free.roll(MakeMap(StrLit(JoinDir.Left.name), LeftSideF)),
          Free.roll(MakeMap(StrLit(JoinDir.Right.name), RightSideF))
        ))

        val equiJoin = quasar.qscript.EquiJoin(src, left, right, key, key, LeftOuter, combine)

        val state: SparkState[RDD[Data]] = alg(equiJoin)
        state.eval(sc).run.unsafePerformSync  must beRightDisjunction.like{
          case rdd =>
            rdd.collect.toList must_= List(
              Data.Obj(ListMap(
                JoinDir.Left.name -> Data.Obj(ListMap(("age" -> Data.Int(32)), ("country" -> Data.Str("Poland")))),
                JoinDir.Right.name -> Data.Null
              )),
              Data.Obj(ListMap(
                JoinDir.Left.name -> Data.Obj(ListMap(("age" -> Data.Int(24)), ("country" -> Data.Str("Poland")))),
                JoinDir.Right.name -> Data.Obj(ListMap(("age" -> Data.Int(24)), ("country" -> Data.Str("US"))))
              ))
              )
        }
        sc.stop
      }).run.unsafePerformSync
      ok
    }

    "equiJoin.rightOuter" in {

      newSc.map ( sc => {
        val alg: AlgebraM[SparkState, EquiJoin, RDD[Data]] = equi.plan(emptyFF)

        val src: RDD[Data] = sc.parallelize(List(
          Data.Obj(ListMap(("age" -> Data.Int(24)), ("country" -> Data.Str("Poland")))),
          Data.Obj(ListMap(("age" -> Data.Int(32)), ("country" -> Data.Str("US")))),
          Data.Obj(ListMap(("age" -> Data.Int(24)), ("country" -> Data.Str("US")))),
          Data.Obj(ListMap(("age" -> Data.Int(14)), ("country" -> Data.Str("UK"))))
        ))

        def func(country: String): FreeMap =
          Free.roll(MapFuncs.Eq(ProjectFieldR(HoleF, StrLit("country")), StrLit(country)))

        def left: FreeQS = Free.roll(QCT.inj(Filter(HoleQS, func("Poland"))))
        def right: FreeQS = Free.roll(QCT.inj(Filter(HoleQS, func("US"))))
        def key: FreeMap = ProjectFieldR(HoleF, StrLit("age"))
        def combine: JoinFunc = Free.roll(ConcatMaps(
          Free.roll(MakeMap(StrLit(JoinDir.Left.name), LeftSideF)),
          Free.roll(MakeMap(StrLit(JoinDir.Right.name), RightSideF))
        ))

        val equiJoin = quasar.qscript.EquiJoin(src, left, right, key, key, RightOuter, combine)

        val state: SparkState[RDD[Data]] = alg(equiJoin)
        state.eval(sc).run.unsafePerformSync  must beRightDisjunction.like{
          case rdd =>
            rdd.collect.toList must_= List(
              Data.Obj(ListMap(
                JoinDir.Left.name ->  Data.Null,
                JoinDir.Right.name -> Data.Obj(ListMap(("age" -> Data.Int(32)), ("country" -> Data.Str("US"))))
              )),
              Data.Obj(ListMap(
                JoinDir.Left.name -> Data.Obj(ListMap(("age" -> Data.Int(24)), ("country" -> Data.Str("Poland")))),
                JoinDir.Right.name -> Data.Obj(ListMap(("age" -> Data.Int(24)), ("country" -> Data.Str("US"))))
              ))
              )
        }
        sc.stop
      }).run.unsafePerformSync
      ok
    }

    "equiJoin.fullOuter" in {

      newSc.map ( sc => {
        val alg: AlgebraM[SparkState, EquiJoin, RDD[Data]] = equi.plan(emptyFF)

        val src: RDD[Data] = sc.parallelize(List(
          Data.Obj(ListMap(("age" -> Data.Int(24)), ("country" -> Data.Str("Poland")))),
          Data.Obj(ListMap(("age" -> Data.Int(27)), ("country" -> Data.Str("Poland")))),
          Data.Obj(ListMap(("age" -> Data.Int(32)), ("country" -> Data.Str("US")))),
          Data.Obj(ListMap(("age" -> Data.Int(24)), ("country" -> Data.Str("US")))),
          Data.Obj(ListMap(("age" -> Data.Int(14)), ("country" -> Data.Str("UK"))))
        ))

        def func(country: String): FreeMap =
          Free.roll(MapFuncs.Eq(ProjectFieldR(HoleF, StrLit("country")), StrLit(country)))

        def left: FreeQS = Free.roll(QCT.inj(Filter(HoleQS, func("Poland"))))
        def right: FreeQS = Free.roll(QCT.inj(Filter(HoleQS, func("US"))))
        def key: FreeMap = ProjectFieldR(HoleF, StrLit("age"))
        def combine: JoinFunc = Free.roll(ConcatMaps(
          Free.roll(MakeMap(StrLit(JoinDir.Left.name), LeftSideF)),
          Free.roll(MakeMap(StrLit(JoinDir.Right.name), RightSideF))
        ))

        val equiJoin = quasar.qscript.EquiJoin(src, left, right, key, key, FullOuter, combine)

        val state: SparkState[RDD[Data]] = alg(equiJoin)
        state.eval(sc).run.unsafePerformSync  must beRightDisjunction.like{
          case rdd =>
            rdd.collect.toList must_= List(
              Data.Obj(ListMap(
                JoinDir.Left.name ->  Data.Null,
                JoinDir.Right.name -> Data.Obj(ListMap(("age" -> Data.Int(32)), ("country" -> Data.Str("US"))))
              )),
              Data.Obj(ListMap(
                JoinDir.Left.name ->  Data.Obj(ListMap(("age" -> Data.Int(27)), ("country" -> Data.Str("Poland")))),
                JoinDir.Right.name -> Data.Null
              )),
              Data.Obj(ListMap(
                JoinDir.Left.name -> Data.Obj(ListMap(("age" -> Data.Int(24)), ("country" -> Data.Str("Poland")))),
                JoinDir.Right.name -> Data.Obj(ListMap(("age" -> Data.Int(24)), ("country" -> Data.Str("US"))))
              ))
              )
        }
        sc.stop
      }).run.unsafePerformSync
      ok
    }

  }

  private def constFreeQS(v: Int): FreeQS =
    Free.roll(QCT.inj(quasar.qscript.Map(Free.roll(QCT.inj(Unreferenced())), IntLit(v))))


  private val emptyFF: (SparkContext, AFile) => Task[RDD[String]] =
    (sc: SparkContext, file: AFile) => Task.delay {
      sc.parallelize(List())
    }

  private def newSc(): OptionT[Task, SparkContext] = for {
    uriStr <- console.readEnv("QUASAR_SPARK_LOCAL")
    uriData <- OptionT(Task.now(DataCodec.parse(uriStr)(DataCodec.Precise).toOption))
    slData <- uriData match {
      case Data.Obj(m) => OptionT(Task.delay(m.get("sparklocal")))
      case _ => OptionT.none[Task, Data]
    }
    uri <- slData match {
      case Data.Obj(m) => OptionT(Task.delay(m.get("connectionUri")))
      case _ => OptionT.none[Task, Data]
    }
    masterAndRoot <- uri match {
      case Data.Str(s) => s.point[OptionT[Task, ?]]
      case _ => OptionT.none[Task, String]
    }
  } yield {
    val master = masterAndRoot.split('|')(0)
    val config = new SparkConf().setMaster(master).setAppName(this.getClass().getName())
    new SparkContext(config)
  }
}
