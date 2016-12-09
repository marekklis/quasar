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

package quasar.physical.couchbase.planner

import quasar.NameGenerator
import quasar.common.PhaseResultT
import quasar.contrib.matryoshka.ShowT
import quasar.physical.couchbase._
import quasar.qscript._

import matryoshka._
import scalaz._

abstract class Planner[F[_], QS[_]] {
  type M[A]  = CBPhaseLog[F, A]
  type PR[A] = PhaseResultT[F, A]

  def plan: AlgebraM[M, QS, N1QL]
}

object Planner {
  def apply[F[_], QS[_]](implicit ev: Planner[F, QS]): Planner[F, QS] = ev

  implicit def coproduct[N[_]: Monad, F[_], G[_]](
    implicit F: Planner[N, F], G: Planner[N, G]
  ): Planner[N, Coproduct[F, G, ?]] =
    new Planner[N, Coproduct[F, G, ?]] {
      val plan: AlgebraM[M, Coproduct[F, G, ?], N1QL] =
        _.run.fold(F.plan, G.plan)
    }

  implicit def constDeadEndPlanner[F[_]: Monad]
    : Planner[F, Const[DeadEnd, ?]] =
    new UnreachablePlanner[F, Const[DeadEnd, ?]]

  implicit def constReadPlanner[F[_]: Monad]
    : Planner[F, Const[Read, ?]] =
    new UnreachablePlanner[F, Const[Read, ?]]

  implicit def constShiftedRead[F[_]: Monad]
    : Planner[F, Const[ShiftedRead, ?]] =
    new ShiftedReadPlanner[F]

  implicit def equiJoinPlanner[F[_]: Monad: NameGenerator, T[_[_]]: Recursive: Corecursive: ShowT]
    : Planner[F, EquiJoin[T, ?]] =
    new EquiJoinPlanner[F, T]

  def mapFuncPlanner[F[_]: Monad: NameGenerator, T[_[_]]: Recursive: ShowT]
    : Planner[F, MapFunc[T, ?]] =
    new MapFuncPlanner[F, T]

  implicit def projectBucketPlanner[F[_]: Monad: NameGenerator, T[_[_]]: Recursive: ShowT]
    : Planner[F, ProjectBucket[T, ?]] =
    new UnreachablePlanner[F, ProjectBucket[T, ?]]

  implicit def qScriptCorePlanner[F[_]: Monad: NameGenerator, T[_[_]]: Recursive: Corecursive: ShowT]
    : Planner[F, QScriptCore[T, ?]] =
    new QScriptCorePlanner[F, T]

  def reduceFuncPlanner[F[_]: Monad]
    : Planner[F, ReduceFunc] =
    new ReduceFuncPlanner[F]

  implicit def thetaJoinPlanner[F[_]: Monad: NameGenerator, T[_[_]]: Recursive: ShowT]
    : Planner[F, ThetaJoin[T, ?]] =
    new UnreachablePlanner[F, ThetaJoin[T, ?]]

}
