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

package quasar.physical.marklogic

import quasar.Predef._
import quasar.Data
import quasar.connector.EnvironmentError
import quasar.contrib.pathy._
import quasar.effect.{KeyValueStore, MonotonicSeq, uuid}
import quasar.fp._, free._
import quasar.fp.numeric.Positive
import quasar.fs._, impl.ReadStream
import quasar.fs.mount._, FileSystemDef.{DefinitionError, DefinitionResult, DefErrT}

import java.net.URI
import scala.util.control.NonFatal

import com.marklogic.xcc._
import com.marklogic.xcc.exceptions._
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

package object fs {
  import ReadFile.ReadHandle, WriteFile.WriteHandle, QueryFile.ResultHandle
  import xcc.{ContentSourceIO, ResultCursor, SessionIO}
  import uuid.GenUUID

  type MLReadHandles[A] = KeyValueStore[ReadHandle, ReadStream[ContentSourceIO], A]
  type MLWriteHandles[A] = KeyValueStore[WriteHandle, Unit, A]
  type MLResultHandles[A] = KeyValueStore[ResultHandle, ResultCursor, A]

  type MarkLogicFs[A] = (
        Task
    :\: SessionIO
    :\: ContentSourceIO
    :\: GenUUID
    :\: MonotonicSeq
    :\: MLReadHandles
    :\: MLWriteHandles
    :/: MLResultHandles
  )#M[A]

  val FsType = FileSystemType("marklogic")

  def contentSourceAt(connectionUri: ConnectionUri): Task[ContentSource] =
    Task.delay(ContentSourceFactory.newContentSource(new URI(connectionUri.value)))

  /** @param readChunkSize the size of a single chunk when streaming records from MarkLogic */
  def definition[S[_]](
    readChunkSize: Positive
  )(implicit
    S0: Task :<: S,
    S1: PhysErr :<: S
  ): FileSystemDef[Free[S, ?]] =
    FileSystemDef.fromPF {
      case (FsType, uri) =>
        EitherT(lift(runMarkLogicFs(uri).run).into[S]) map { case (run, shutdown) =>
          DefinitionResult[Free[S, ?]](
            mapSNT(injectNT[Task, S] compose run) compose interpretFileSystem(
              queryfile.interpret[MarkLogicFs](readChunkSize),
              readfile.interpret[MarkLogicFs](readChunkSize),
              writefile.interpret[MarkLogicFs],
              managefile.interpret[MarkLogicFs]),
            lift(shutdown).into[S])
        }
    }

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def runMarkLogicFs(connectionUri: ConnectionUri): DefErrT[Task, (MarkLogicFs ~> Task, Task[Unit])] = {
    val contentSource: DefErrT[Task, ContentSource] =
      EitherT(contentSourceAt(connectionUri) map (_.right[DefinitionError]) handle {
        case NonFatal(t) => t.getMessage.wrapNel.left[EnvironmentError].left
      })

    (
      KeyValueStore.impl.empty[WriteHandle, Unit]                       |@|
      KeyValueStore.impl.empty[ReadHandle, ReadStream[ContentSourceIO]] |@|
      KeyValueStore.impl.empty[ResultHandle, ResultCursor]              |@|
      MonotonicSeq.fromZero                                             |@|
      GenUUID.type1
    ).tupled.liftM[DefErrT].flatMap { case (whandles, rhandles, qhandles, seq, genUUID) =>
      contentSource.flatMapF { cs =>
        val runCSIO = ContentSourceIO.runNT(cs)
        val runSIO  = runCSIO compose ContentSourceIO.runSessionIO
        val runML   = reflNT[Task] :+: runSIO :+: runCSIO :+: genUUID :+: seq :+: rhandles :+: whandles :+: qhandles
        val sdown   = Task.delay(cs.getConnectionProvider.shutdown(null))

        // NB: An innocuous operation used to test the connection.
        runSIO(SessionIO.currentServerPointInTime)
          .as((runML, sdown).right[DefinitionError])
          .handle {
            case ex: RequestPermissionException =>
              EnvironmentError.invalidCredentials(ex.getMessage).right.left

            case NonFatal(th) =>
              EnvironmentError.connectionFailed(th).right.left
          }
      }
    }
  }

  implicit val resultCursorDataCursor: DataCursor[Task, ResultCursor] =
    new DataCursor[Task, ResultCursor] {
      def close(rc: ResultCursor) =
        rc.close.void

      def nextChunk(rc: ResultCursor) =
        TV.map(rc.nextChunk)(xdm => xdmitem.toData[ErrorMessages \/ ?](xdm) | Data.NA)

      val TV = Functor[Task].compose[Vector]
    }

  def asDir(file: AFile): ADir =
    fileParent(file) </> dir(fileName(file).value)

  def pathUri(path: APath): String =
    posixCodec.printPath(path)
}
