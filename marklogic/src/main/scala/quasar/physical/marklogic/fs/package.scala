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

import quasar.Predef.{uuid => _, _}
import quasar.{Data, Planner => QPlanner}
import quasar.common._
import quasar.connector.EnvironmentError
import quasar.contrib.pathy._
import quasar.effect._
import quasar.fp._, eitherT._, free._
import quasar.fp.numeric.Positive
import quasar.frontend.logicalplan, logicalplan.LogicalPlan
import quasar.fs._
import quasar.fs.mount._, FileSystemDef.{DefinitionError, DefinitionResult, DefErrT}
import quasar.physical.marklogic.fmt._
import quasar.physical.marklogic.qscript._
import quasar.physical.marklogic.xcc.AsContent
import quasar.physical.marklogic.xquery.Prologs

import java.net.URI
import java.util.UUID
import scala.util.control.NonFatal

import com.marklogic.xcc._
import com.marklogic.xcc.exceptions._
import matryoshka.data.Fix
import pathy.Path._
import scalaz.{Failure => _, _}, Scalaz.{ToIdOps => _, _}
import scalaz.concurrent.Task

package object fs {
  import ReadFile.ReadHandle, WriteFile.WriteHandle, QueryFile.ResultHandle
  import queryfile.MLQScript
  import xcc._
  import uuid.GenUUID

  type MLReadHandles[A]     = KeyValueStore[ReadHandle, Option[ResultCursor], A]
  type MLWriteHandles[A]    = KeyValueStore[WriteHandle, Unit, A]
  type MLResultHandles[A]   = KeyValueStore[ResultHandle, ResultCursor, A]
  type XccSessionR[A]       = Read[Session, A]
  type XccContentSourceR[A] = Read[ContentSource, A]
  type XccFailure[A]        = Failure[XccError, A]

  type MarkLogicFs[A] = (
        Task
    :\: XccSessionR
    :\: XccContentSourceR
    :\: XccFailure
    :\: GenUUID
    :\: MonotonicSeq
    :\: MLReadHandles
    :\: MLWriteHandles
    :/: MLResultHandles
  )#M[A]

  type MLFS[A]  = WriterT[Free[MarkLogicFs, ?], Prologs, A]
  type MLFSQ[A] = MarkLogicPlanErrT[MLFS, A]

  private implicit val mlfsSessionR = Read.monadReader_[Session, MarkLogicFs]
  private implicit val mlfsCSourceR = Read.monadReader_[ContentSource, MarkLogicFs]
  private implicit val mlfsUuidR    = Read.monadReader_[UUID, MarkLogicFs]
  private implicit val mlfsXccError = Failure.monadError_[XccError, MarkLogicFs]

  val FsType = FileSystemType("marklogic")

  /** The result of viewing the given file as a directory, converting the
    * file name to a direcotry name.
    */
  def asDir(file: AFile): ADir =
    fileParent(file) </> dir(fileName(file).value)

  /** The `ContentSource` located at the given URI. */
  def contentSourceAt[F[_]: Capture](connectionUri: ConnectionUri): F[ContentSource] =
    Capture[F].delay(ContentSourceFactory.newContentSource(new URI(connectionUri.value)))

  /** The MarkLogic FileSystem definition.
    *
    * @tparam FMT           type representing the document format for the filesystem
    * @param  readChunkSize the size of a single chunk when streaming records from MarkLogic
    */
  def definition[S[_], FMT](
    readChunkSize: Positive
  )(implicit
    S0: Task :<: S,
    S1: PhysErr :<: S,
    C: AsContent[FMT, Data],
    P: Planner[MLFSQ, FMT, MLQScript[Fix, ?]]
  ): FileSystemDef[Free[S, ?]] = {
    val dropWritten = λ[MLFS ~> Free[MarkLogicFs, ?]](_.value)

    FileSystemDef fromPF {
      case (FsType, uri) =>
        runMarkLogicFs(uri) map { case (run, shutdown) =>
          DefinitionResult[Free[S, ?]](
            flatMapSNT(run) compose dropWritten compose interpretFileSystem(
              handleMLPlannerErrors(queryfile.interpret[MLFSQ, FMT](readChunkSize)),
              readfile.interpret[MLFS](readChunkSize),
              writefile.interpret[MLFS, FMT],
              managefile.interpret[MLFS]),
            shutdown)
        }
    }
  }

  // NB: These exists due to the awkward need to import the MonadReader_ and
  //     MonadError_ definitions for Read/Failure. Given some reorganization
  //     this could eventually go away.

  def definitionJson[S[_]](
    readChunkSize: Positive
  )(implicit
    S0: Task :<: S,
    S1: PhysErr :<: S
  ): FileSystemDef[Free[S, ?]] =
    definition[S, fmt.JSON](readChunkSize)

  def definitionXml[S[_]](
    readChunkSize: Positive
  )(implicit
    S0: Task :<: S,
    S1: PhysErr :<: S
  ): FileSystemDef[Free[S, ?]] =
    definition[S, fmt.XML](readChunkSize)

  def pathUri(path: APath): String =
    posixCodec.printPath(path)

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def runMarkLogicFs[S[_]](
    connectionUri: ConnectionUri
  )(implicit
    S0: Task :<: S,
    S1: PhysErr :<: S
  ): DefErrT[Free[S, ?], (MarkLogicFs ~> Free[S, ?], Free[S, Unit])] = {
    val contentSource: DefErrT[Free[S, ?], ContentSource] =
      EitherT(lift(contentSourceAt[Task](connectionUri) map (_.right[DefinitionError]) handle {
        case NonFatal(t) => t.getMessage.wrapNel.left[EnvironmentError].left
      }).into[S])

    def provideSession(cs: ContentSource) = λ[XccSessionR ~> Free[S, ?]] {
      case Read.Ask(f) => contentsource.newSession[ReaderT[Free[S, ?], ContentSource, ?]](None).run(cs) map f
    }

    val runFs = (
      KeyValueStore.impl.empty[WriteHandle, Unit]                |@|
      KeyValueStore.impl.empty[ReadHandle, Option[ResultCursor]] |@|
      KeyValueStore.impl.empty[ResultHandle, ResultCursor]       |@|
      MonotonicSeq.fromZero                                      |@|
      GenUUID.type1
    ).tupled.map { case (whandles, rhandles, qhandles, seq, genUUID) =>
      contentSource flatMapF { cs =>
        val runCSrc = Read.constant[Free[S, ?], ContentSource](cs)
        val runSess = provideSession(cs)
        val runErrs = injectFT[PhysErr, S] compose handleXccErrors
        val runML   = injectFT[Task, S]                    :+:
                      runSess                              :+:
                      runCSrc                              :+:
                      runErrs                              :+:
                      (injectFT[Task, S] compose genUUID)  :+:
                      (injectFT[Task, S] compose seq)      :+:
                      (injectFT[Task, S] compose rhandles) :+:
                      (injectFT[Task, S] compose whandles) :+:
                      (injectFT[Task, S] compose qhandles)
        val sdown   = lift(Task.delay(cs.getConnectionProvider.shutdown(null))).into[S]

        val tested  = contentsource.newSession[ReaderT[Task, ContentSource, ?]](None)
                        .run(cs)
                        .flatMap(
                          testXccConnection[ReaderT[Task, Session, ?]]
                            .map(_.right[NonEmptyList[String]])
                            .toLeft((runML, sdown))
                            .run)
                        .handle {
                          case NonFatal(th) => EnvironmentError.connectionFailed(th).right.left
                        }

        lift(tested).into[S]
      }
    }

    lift(runFs).into[S].liftM[DefErrT].join
  }

  /** Returns an error describing why an XCC connection failed or `None` if it
    * is successful.
    */
  def testXccConnection[F[_]: Monad: Capture: SessionReader]: OptionT[F, EnvironmentError] =
    // NB: An innocuous operation used to test the connection.
    session.currentServerPointInTime[EitherT[F, XccError, ?]].swap.toOption.map {
      case XccError.RequestError(ex: RequestPermissionException) =>
        EnvironmentError.invalidCredentials(ex.getMessage)

      case other =>
        EnvironmentError.connectionFailed(XccError.cause.get(other))
    }

  implicit val dataAsXmlContent: AsContent[XML, Data] =
    new AsContent[XML, Data] {
      def asContent[F[_]: MonadErrMsgs](uri: ContentUri, d: Data): F[Content] =
        data.encodeXml[F](d) map { elem =>
          val opts = new ContentCreateOptions
          opts.setFormatXml()
          ContentFactory.newContent(uri.get, elem.toString, opts)
        }
    }

  implicit val dataAsJsonContent: AsContent[JSON, Data] =
    new AsContent[JSON, Data] {
      def asContent[F[_]: MonadErrMsgs](uri: ContentUri, d: Data): F[Content] = {
        val opts = new ContentCreateOptions
        opts.setFormatJson()
        ContentFactory.newContent(uri.get, data.encodeJson(d).nospaces, opts).point[F]
      }
    }

  implicit def resultCursorDataCursor[F[_]: Capture: Monad]: DataCursor[F, ResultCursor] =
    new DataCursor[F, ResultCursor] {
      def close(rc: ResultCursor) =
        rc.close[F].void

      def nextChunk(rc: ResultCursor) =
        FV.map(rc.nextChunk[F])(xdm => xdmitem.toData[ErrorMessages \/ ?](xdm) | Data.NA)

      val FV = Functor[F].compose[Vector]
    }

  ////

  /** Converts MarkLogicPlannerErrors into FileSystemErrors. */
  private def handleMLPlannerErrors[F[_]: Functor](f: QueryFile ~> MarkLogicPlanErrT[F, ?]): QueryFile ~> F = {
    import QueryFile._, MarkLogicPlannerError._

    def asFsError[A](
      lp: Fix[LogicalPlan],
      fa: MarkLogicPlanErrT[F, (PhaseResults, FileSystemError \/ A)]
    ): F[(PhaseResults, FileSystemError \/ A)] = fa.run map {
      case -\/(mlerr @ InvalidQName(s)) =>
        (Vector[PhaseResult](), -\/(FileSystemError.planningFailed(lp, QPlanner.UnsupportedPlan(
          // TODO: Change to include the QScript context when supported
          logicalplan.constant(Data.Str(s)), Some((mlerr: MarkLogicPlannerError).shows)))))

      case -\/(Unreachable(d)) =>
        (Vector[PhaseResult](), -\/(FileSystemError.planningFailed(lp, QPlanner.InternalError(
          s"Should not have been reached, indicates a defect: $d.",
          None))))

      case \/-(r) => r
    }

    λ[QueryFile ~> F] {
      case ExecutePlan(lp, file) => asFsError(lp, f(ExecutePlan(lp, file)))
      case EvaluatePlan(lp)      => asFsError(lp, f(EvaluatePlan(lp)))
      case More(h)               => f(More(h)) | Vector[Data]().right[FileSystemError]
      case Close(h)              => f(Close(h)) | (())
      case Explain(lp)           => asFsError(lp, f(Explain(lp)))
      case ListContents(dir)     => f(ListContents(dir)) | Set[PathSegment]().right[FileSystemError]
      case FileExists(file)      => f(FileExists(file)) | false
    }
  }

  private val handleXccErrors: XccFailure ~> PhysErr = λ[XccFailure ~> PhysErr] {
    case Failure.Fail(XccError.RequestError(ex)) =>
      Failure.Fail(PhysicalError.unhandledFSError(ex))

    case Failure.Fail(xe @ XccError.XQueryError(xqy, ex)) =>
      Failure.Fail(PhysicalError.unhandledFSError(
        new RuntimeException((xe: XccError).shows, ex)))
  }
}
