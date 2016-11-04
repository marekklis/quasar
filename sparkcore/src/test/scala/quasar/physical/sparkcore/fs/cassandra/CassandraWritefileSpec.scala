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
import quasar.Data
import quasar.effect._
import quasar.fp.free._
import quasar.fp.TaskRef
import quasar.fs._, WriteFile.WriteHandle

import org.apache.spark._
import pathy._, Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

class CassandraWritefileSpec extends quasar.Qspec {

	type Eff[A] = Coproduct[MonotonicSeq, Read[SparkContext, ?], A]

	"writefile" should {

		"create table if not exists" in {
			val aFile = rootDir </> dir("foo12") </> file("bar12")
			val program: Free[Eff, FileSystemError \/ WriteHandle] = writefile.open(aFile)

			val sc = newSc()

			def monotonicSeqInter: Task[MonotonicSeq ~> Task] = TaskRef(0L).map( MonotonicSeq.fromTaskRef _ )

 	 		val readInter: Read[SparkContext, ?] ~> Task = Read.constant[Task, SparkContext](sc)
			
			val interpreter: Task[Eff ~> Task] = monotonicSeqInter.map(_ :+: readInter)

			val result: FileSystemError \/ WriteHandle = 
				interpreter.map(program foldMap _).join.unsafePerformSync

			sc.stop()
			ok
		}

		"write should insert data into table" in {
			val aFile = rootDir </> dir("foo") </> file("bar")
			val data = (user("john", 31) :: user("anna", 21) :: Nil).toVector
			val openFileProgram: Free[Eff, FileSystemError \/ WriteHandle] = writefile.open(aFile)
			val writeProgram: Free[Eff, Vector[FileSystemError]] = writefile.write(WriteHandle(aFile, 1L), data)
			val sc = newSc()

			def monotonicSeqInter: Task[MonotonicSeq ~> Task] = TaskRef(0L).map( MonotonicSeq.fromTaskRef _ )

 	 		val readInter: Read[SparkContext, ?] ~> Task = Read.constant[Task, SparkContext](sc)
			
			val interpreter: Task[Eff ~> Task] = monotonicSeqInter.map(_ :+: readInter)

			val result: Vector[FileSystemError] = 
				interpreter
					.map(writeProgram foldMap _)
					.join
					.unsafePerformSync
			sc.stop()

			result must_= Vector.empty[FileSystemError]
		}
	}

	  private def newSc(): SparkContext = {
    val config = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass().getName())
      .set("spark.cassandra.connection.host", "localhost")

    new SparkContext(config)
  }

  private def user(login: String, age: Int) =
    Data.Obj(ListMap("login" -> Data.Str(login), "age" -> Data.Int(age)))
	
}
