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

package quasar

import quasar.Predef._
import quasar.fp.ski._
import quasar.contrib.matryoshka._

import matryoshka.{Delay, Fix}
import scalaz._
import simulacrum.typeclass

/** Analogous to `ShowT`; allows construction of `Delay[RenderTree, F]` for
  * an `F` that refers to `T[... F ...]`. */
@typeclass trait RenderTreeT[T[_[_]]] {
  def render[F[_]: Functor](t: T[F])(implicit delay: Delay[RenderTree, F]): RenderedTree

  def renderTree[F[_]: Functor](delay: Delay[RenderTree, F]): RenderTree[T[F]] =
    RenderTree.make[T[F]](t => render(t)(Functor[F], delay))
}
object RenderTreeT {
  implicit val fix: RenderTreeT[Fix] =
    new RenderTreeT[Fix] {
      def render[F[_]: Functor](t: Fix[F])(implicit delay: Delay[RenderTree, F]): RenderedTree =
        delay(renderTree[F](delay)).render(t.unFix)
    }

  // NB: no point making it implicit because the separation of F and A defeats
  // implicit search.
  def free[A: RenderTree]: RenderTreeT[Free[?[_], A]] = new RenderTreeT[Free[?[_], A]] {
    def render[F[_]: Functor](t: Free[F, A])(implicit delay: Delay[RenderTree, F]): RenderedTree =
      freeCata(t)(interpret[F, A, RenderedTree](
        a => RenderTree[A].render(a),
        f => delay(RenderTree.make[RenderedTree](ι)).render(f)))
  }
}
