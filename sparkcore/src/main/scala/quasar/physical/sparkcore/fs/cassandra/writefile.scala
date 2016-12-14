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
import quasar.{Data, DataCodec, DataEncodingError}
import quasar.effect._
import quasar.fs._, WriteFile._, FileSystemError._
import quasar.fp.free._

import org.apache.spark.SparkContext
import com.datastax.driver.core.Session
import com.datastax.spark.connector.cql.CassandraConnector
import pathy.Path.{ fileParent, posixCodec }
import scalaz._, Scalaz._


object writefile {

  def chrooted[S[_]](prefix: ADir)(implicit
    s0: MonotonicSeq :<: S,
    s1: Read.Ops[SparkContext, S]
  ) : WriteFile ~> Free[S, ?] =
    flatMapSNT(interpret) compose chroot.writeFile[WriteFile](prefix)

  def interpret[S[_]](implicit 
    s0: MonotonicSeq :<: S,
    s1: Read.Ops[SparkContext, S]
    ): WriteFile ~> Free[S, ?] = {
    new (WriteFile ~> Free[S, ?]) {
      def apply[A](wr: WriteFile[A]): Free[S, A] = wr match {
        case Open(f) => open(f)
        case Write(h, ch) => write(h, ch)
        case Close(h) => close(h)
      }
    }
  }

  def open[S[_]](file: AFile)(implicit 
    gen: MonotonicSeq.Ops[S],
    s1: Read.Ops[SparkContext, S]
    ): Free[S, FileSystemError \/ WriteHandle] = 
    for {
      id <- gen.next
      _ <- create(file)
    } yield (WriteHandle(file, id).right)

  def create[S[_]](file: AFile)(implicit
    read: Read.Ops[SparkContext, S]
    ): Free[S, Unit] = 
    read.asks { sc =>
      val connector = CassandraConnector(sc.getConf)
      connector.withSessionDo { implicit session =>
        val k = if(!keyspaceExists(keyspace(file))) {
          session.execute(s"CREATE KEYSPACE ${keyspace(file)} WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};")
        }

        val t = if(!tableExists(keyspace(file), tableName(file)))
          createTable(keyspace(file), tableName(file))

          ()
      }
    }

  private def keyspace(file: AFile) =
    posixCodec.printPath(fileParent(file)).substring(1).replace("/", "_")

  private def tableName(file: AFile) =
    posixCodec.printPath(file).split("/").reverse(0)

  private def keyspaceExists(keyspace: String)(implicit session: Session) = {
    val stmt = session.prepare("SELECT * FROM system_schema.keyspaces WHERE keyspace_name = ?;")
    session.execute(stmt.bind(keyspace)).all().size() > 0
  }

  private def tableExists(keyspace: String, table: String)(implicit session: Session) = {
    val stmt = session.prepare("SELECT * FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?;")
    session.execute(stmt.bind(keyspace, table)).all().size() > 0
  }

  private def createTable(keyspace: String, table: String)(implicit session: Session) = {
    session.execute(s"CREATE TABLE $keyspace.$table (id timeuuid PRIMARY KEY, data text);")
  }

  private def insertData(keyspace: String, table: String, data: String)(implicit session: Session) = {
    val stmt = session.prepare(s"INSERT INTO $keyspace.$table (id, data) VALUES (now(),  ?);")
    session.execute(stmt.bind(data))
  }

  def write[S[_]](handle: WriteHandle, data: Vector[Data])(implicit
    read: Read.Ops[SparkContext, S]
    ): Free[S, Vector[FileSystemError]] = 
  read.asks{ sc =>
    implicit val codec = DataCodec.Precise
    val textChunk: Vector[(DataEncodingError \/ String, Data)] = data.map(d => (DataCodec.render(d), d))
      
    CassandraConnector(sc.getConf).withSessionDo {implicit session =>
      textChunk.flatMap{
        case (\/-(text),data) => 
          \/.fromTryCatchNonFatal{
            insertData(keyspace(handle.file), tableName(handle.file), text)
          }.fold(
            ex => Vector(writeFailed(data, ex.getMessage)),
            u => Vector.empty[FileSystemError]
          )
        case (-\/(error), data) => Vector(writeFailed(data, error.message))
      }
    }
  }

  def close[S[_]](handle: WriteHandle): Free[S, Unit] = ().point[Free[S, ?]]
}
