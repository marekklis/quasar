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

import quasar.effect.Read
import quasar.Predef._
import quasar.fp.numeric._
import quasar.effect._

import pathy._, Path._
import org.apache.spark._
import org.apache.spark.rdd._
import scalaz._, scalaz.concurrent.Task

class CassandraReadfileSpec extends quasar.Qspec {

  type ReadSpark[A] = Read[SparkContext, A]

  "fileread" should {

    "fileExists" in {
      val program: Free[ReadSpark, Boolean] = readfile.fileExists[ReadSpark](rootDir </> dir("foo") </> file("bar"))
      val sc = newSc()
      val interpreter: ReadSpark ~> Task = Read.constant[Task, SparkContext](sc)

      val result = (program foldMap interpreter).unsafePerformSync
      sc.stop()
      result must_== true
    }

    "rddFrom" in {
      val program: Free[ReadSpark, RDD[String]] = readfile.rddFrom[ReadSpark](rootDir </> dir("foo") </> file("bar"), Natural(0).get, None)
      val sc = newSc()
      val interpreter: ReadSpark ~> Task = Read.constant[Task, SparkContext](sc)

      val results: Array[String] = ((program foldMap interpreter).unsafePerformSync).collect()
      
      results.size must be_==(2)
      results.toSeq must contain("{\"login\": \"john\",\"age\":\"28\"}")
      results.toSeq must contain("{\"login\": \"kate\",\"age\":\"31\"}")

      sc.stop()
      ok
    }
  }

  private def newSc(): SparkContext = {
    val config = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass().getName())
      .set("spark.cassandra.connection.host", "localhost")

    new SparkContext(config)
  }

}
