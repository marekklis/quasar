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
import quasar.contrib.pathy._
import quasar.connector.EnvironmentError
import quasar.effect._
import quasar.fp.free._
import quasar.fp.TaskRef
import quasar.fs._, QueryFile.ResultHandle, ReadFile.ReadHandle
import quasar.fs.mount._, FileSystemDef._
import quasar.physical.sparkcore.fs.{queryfile => corequeryfile, readfile => corereadfile}

import scala.sys

import org.apache.spark._
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

package object cassandra {

	import corequeryfile.RddState

	val FsType = FileSystemType("spark-cassandra")

	type EffM1[A] = Coproduct[KeyValueStore[ResultHandle, RddState, ?], Read[SparkContext, ?], A]
  type Eff0[A] = Coproduct[KeyValueStore[ReadHandle, SparkCursor, ?], EffM1, A]
  type Eff1[A] = Coproduct[Task, Eff0, A]
  type Eff2[A] = Coproduct[PhysErr, Eff1, A]
  type Eff[A]  = Coproduct[MonotonicSeq, Eff2, A]

	final case class SparkFSConf(sparkConf: SparkConf, prefix: ADir)

  def parseUri(uri: ConnectionUri): DefinitionError \/ SparkFSConf = {

    def error(msg: String): DefinitionError \/ SparkFSConf =
      NonEmptyList(msg).left[EnvironmentError].left[SparkFSConf]

    def forge(master: String, cassandraHost: String, rootPath: String): DefinitionError \/ SparkFSConf =
      posixCodec.parseAbsDir(rootPath)
        .map { prefix =>
        SparkFSConf(new SparkConf().setMaster(master).setAppName("quasar").set("spark.cassandra.connection.host", cassandraHost), sandboxAbs(prefix))
      }.fold(error(s"Could not extrat a path from $rootPath"))(_.right[DefinitionError])

    uri.value.split('|').toList match {
      case master :: cassandraHost :: prefixPath :: Nil => forge(master, cassandraHost, prefixPath)
      case _ =>
        error("Missing master and prefixPath (seperated by |)" +
          " e.g spark://spark_host:port|cassandra://cassandra_host:port|/user/")
    }
  }

  final case class SparkCassandraFSDef[S[_]](run: Free[Eff, ?] ~> Free[S, ?], close: Free[S, Unit])

  private def fetchSparkCoreJar: Task[String] = Task.delay {
    sys.env("QUASAR_HOME") + "/sparkcore.jar"
  }

  private def sparkFsDef[S[_]](sparkConf: SparkConf)(implicit
    S0: Task :<: S,
    S1: PhysErr :<: S
  ): Free[S, SparkCassandraFSDef[S]] = {

    val genSc = fetchSparkCoreJar.map { jar => 
      val sc = new SparkContext(sparkConf)
      sc.addJar(jar)
      sc
    }

    lift((TaskRef(0L) |@|
      TaskRef(Map.empty[ResultHandle, RddState]) |@|
      TaskRef(Map.empty[ReadHandle, SparkCursor]) |@|
      genSc) {
      // TODO better names!
      (genState, rddStates, sparkCursors, sc) =>
      val interpreter: Eff ~> S = (MonotonicSeq.fromTaskRef(genState) andThen injectNT[Task, S]) :+:
      injectNT[PhysErr, S] :+:
      injectNT[Task, S]  :+:
      (KeyValueStore.impl.fromTaskRef[ReadHandle, SparkCursor](sparkCursors) andThen injectNT[Task, S]) :+:
      (KeyValueStore.impl.fromTaskRef[ResultHandle, RddState](rddStates) andThen injectNT[Task, S]) :+:
      (Read.constant[Task, SparkContext](sc) andThen injectNT[Task, S])

      SparkCassandraFSDef(mapSNT[Eff, S](interpreter), lift(Task.delay(sc.stop())).into[S])
    }).into[S]
  }

  private def fsInterpret(sparkFsConf: SparkFSConf): FileSystem ~> Free[Eff, ?] = interpretFileSystem(
      corequeryfile.chrooted[Eff](queryfile.input, FsType, sparkFsConf.prefix),
      corereadfile.chrooted(readfile.input[Eff], sparkFsConf.prefix),
      writefile.chrooted[Eff](sparkFsConf.prefix),
      managefile.chrooted[Eff](sparkFsConf.prefix))

	def definition[S[_]](implicit S0: Task :<: S, S1: PhysErr :<: S):
      FileSystemDef[Free[S, ?]] =
    FileSystemDef.fromPF {
      case (FsType, uri) =>
        for {
          sparkFsConf <- EitherT(parseUri(uri).point[Free[S, ?]])
          res <- {
            sparkFsDef(sparkFsConf.sparkConf).map {
              case SparkCassandraFSDef(run, close) =>
                FileSystemDef.DefinitionResult[Free[S, ?]](
                  fsInterpret(sparkFsConf) andThen run,
                  close)
            }.liftM[DefErrT]
          }
        }  yield res
    }
}
