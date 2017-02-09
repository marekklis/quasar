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
import quasar.fs._,
  ManageFile._,
  MoveScenario._,
  FileSystemError._,
  PathError._
import quasar.effect._
import quasar.fp.free._
import quasar.fs.impl.ensureMoveSemantics

import org.apache.spark.SparkContext
import com.datastax.driver.core.Session
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import pathy._, Path._
import scalaz._, Scalaz._

object managefile {

  def chrooted[S[_]](prefix: ADir)(implicit
    s0: Read.Ops[SparkContext, S]
  ): ManageFile ~> Free[S, ?] =
    flatMapSNT(interpret) compose chroot.manageFile[ManageFile](prefix)
  
  def interpret[S[_]](implicit
    s0: Read.Ops[SparkContext, S]
    ): ManageFile ~> Free[S, ?] = 
    new (ManageFile ~> Free[S, ?]) {
      def apply[A](mf: ManageFile[A]): Free[S, A] = mf match {
        case Move(FileToFile(sf, df), semantics) =>
          (ensureMoveSemantics[Free[S, ?]](sf, df, pathExists _, semantics).toLeft(()) *>
            moveFile(sf, df).liftM[FileSystemErrT]).run
        case Move(DirToDir(sd, dd), semantics) =>
          (ensureMoveSemantics[Free[S, ?]](sd, dd, pathExists _, semantics).toLeft(()) *>
            moveDir(sd, dd).liftM[FileSystemErrT]).run
        case Delete(path) => delete(path)
        case TempFile(near) => tempFile(near)
      }
    }

  def pathExists[S[_]](path: APath)(implicit 
    read: Read.Ops[SparkContext, S]
    ): Free[S, Boolean] = 
    read.asks { sc =>
      CassandraConnector(sc.getConf).withSessionDo { implicit session =>
        maybeFile(path)
          .fold(keyspaceExists(keyspace(path)))(file => tableExists(keyspace(fileParent(file)), tableName(file)))
    }
  }

  def moveFile[S[_]](sf: AFile, df: AFile)(implicit 
    read: Read.Ops[SparkContext, S]
    ): Free[S, Unit] = {
    read.asks { implicit sc =>
      moveTable(sf, df)
    }
  }

  private def moveTable(sf: AFile, df: AFile)(implicit sc: SparkContext) = {
    sc.cassandraTable(keyspace(fileParent(sf)), tableName(sf))
      .saveToCassandra(keyspace(fileParent(df)), tableName(df), SomeColumns("id", "data"))
    CassandraConnector(sc.getConf).withSessionDo { implicit session =>
      val r = dropTable(keyspace(fileParent(sf)), tableName(sf))
    }
  }

  def moveDir[S[_]](sd: ADir, dd: ADir)(implicit 
    read: Read.Ops[SparkContext, S]
    ): Free[S, Unit] = {
    read.asks { implicit sc =>
      sc.cassandraTable[String]("system_schema", "tables").select("table_name").foreach {tn: String=>
        moveTable(sd </> file(tn), dd </> file(tn))
      }
    }
  }

  def delete[S[_]](path: APath)(implicit
    read: Read.Ops[SparkContext, S]
    ): Free[S, FileSystemError \/ Unit] = 
  read.asks { sc =>
    CassandraConnector(sc.getConf).withSessionDo { implicit session =>
      maybeFile(path).fold(deleteDir(path))(file => deleteFile(file))
    }
  }

  def deleteFile(file: AFile)(implicit session: Session) = 
    if(keyspaceExists(keyspace(fileParent(file))) && tableExists(keyspace(fileParent(file)), tableName(file))) {
      val r = dropTable(keyspace(file), tableName(file))
      ().right[FileSystemError]
    } else {
      pathErr(pathNotFound(file)).left[Unit]
    }

  def deleteDir(dir: APath)(implicit session: Session) =  
    if(keyspaceExists(keyspace(dir))) {
      val r = dropKeyspace(keyspace(dir))
      ().right[FileSystemError]
    } else {
      pathErr(pathNotFound(dir)).left[Unit]
    }

  def tempFile[S[_]](near: APath)(implicit
    read: Read.Ops[SparkContext, S]
    ): Free[S, FileSystemError \/ AFile] = 
    read.asks { sc =>
      CassandraConnector(sc.getConf).withSessionDo { implicit session =>
        val randomFileName = s"quasar${scala.math.abs(scala.util.Random.nextInt())}"
        val aDir: ADir = refineType(near).fold(d => d, fileParent(_))
        if (!keyspaceExists(keyspace(near))) {
          val r = createTable(keyspace(near), randomFileName)
          (aDir </> file(randomFileName)).right[FileSystemError]
        } else {
          pathErr(pathNotFound(near)).left[AFile]
        }
      }
    }

  private def keyspace(dir: APath) =
    posixCodec.printPath(dir).substring(1).replace("/", "_")

  private def tableName(file: APath) =
    posixCodec.printPath(file).split("/").reverse(0)

  private def keyspaceExists(keyspace: String)(implicit session: Session) = {
    val stmt = session.prepare("SELECT * FROM system_schema.keyspaces WHERE keyspace_name = ?;")
    session.execute(stmt.bind(keyspace)).all().size() > 0
  }

  private def tableExists(keyspace: String, table: String)(implicit session: Session) = {
    val stmt = session.prepare("SELECT * FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?;")
    session.execute(stmt.bind(keyspace, table)).all().size() > 0
  }

  private def dropKeyspace(keyspace: String)(implicit session: Session) = {
    session.execute(s"DROP KEYSPACE $keyspace;")
  }

  private def dropTable(keyspace: String, table: String)(implicit session: Session) = {
    session.execute(s"DROP TABLE $keyspace.$table;")
  }

  private def createTable(keyspace: String, table: String)(implicit session: Session) = {
    session.execute(s"CREATE TABLE $keyspace.$table (id timeuuid PRIMARY KEY, data text);")
  }
}
