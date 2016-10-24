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

package quasar.physical.sparkcore.fs.cassandra

import quasar.Predef._
import quasar.contrib.pathy._
import quasar.effect.Read
import quasar.physical.sparkcore.fs.readfile.{ Offset, Limit }
import quasar.physical.sparkcore.fs.readfile.Input

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import pathy.Path.{ fileParent, posixCodec }
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import scalaz._
import scalaz.concurrent.Task

object readfile {

  def rddFrom[S[_]](f: AFile, offset: Offset, maybeLimit: Limit)(implicit read: Read.Ops[SparkContext, S]): Free[S, RDD[String]] =
    read.asks { sc =>
      sc.cassandraTable[String](keyspace(f), tableName(f))
        .select("data")
        .zipWithIndex()
        .filter {
          case (value, index) =>
            maybeLimit.fold(
              index >= offset.get)(
                limit => index >= offset.get && index < limit.get + offset.get)
        }.map {
          case (value, index) => value
        }
    }

  def fileExists[S[_]](f: AFile)(implicit read: Read.Ops[SparkContext, S]): Free[S, Boolean] =
    read.asks { sc =>
      val connector = CassandraConnector(sc.getConf)
      connector.withSessionDo { implicit session =>
        val stmt = session.prepare("SELECT * FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?;")
        session.execute(stmt.bind(keyspace(f), tableName(f))).all().size() > 0
      }
    }

  // TODO arbitrary value, more or less a good starting point
  // but we should consider some measuring
  def readChunkSize: Int = 5000

  def input[S[_]](implicit read: Read.Ops[SparkContext, S], s0: Task :<: S) =
    Input(
      rddFrom(_, _, _),
      fileExists(_),
      readChunkSize _)

  private def keyspace(file: AFile) =
    posixCodec.printPath(fileParent(file)).substring(1).replace("/", "_")

  private def tableName(file: AFile) =
    posixCodec.printPath(file).split("/").reverse(0)

}
