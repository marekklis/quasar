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

package quasar.physical.marklogic.xml

import quasar.physical.marklogic.validation._

import eu.timepit.refined.api.Refined
import org.scalacheck.{Arbitrary, Gen}, Arbitrary.arbitrary

trait Arbitraries {
  import Arbitraries._

  /** @see https://www.w3.org/TR/2009/REC-xml-names-20091208/#NT-NCName */
  implicit val arbitraryNCName: Arbitrary[NCName] =
    Arbitrary(for {
      st <- Gen.oneOf(ncNameStartChars)
      ss <- Gen.listOf(Gen.oneOf(ncNameChars))
    } yield NCName(Refined.unsafeApply((st :: ss).mkString)))

  /** @see https://www.w3.org/TR/2009/REC-xml-names-20091208/#NT-QName */
  implicit val arbitraryQName: Arbitrary[QName] =
    Arbitrary(for {
      loc <- arbitrary[NCName]
      pfx <- arbitrary[NCName]
      qn  <- Gen.oneOf(QName.local(loc), QName.prefixed(NSPrefix(pfx), loc))
    } yield qn)
}

object Arbitraries extends Arbitraries {
  private val ncNameStartChars = NCNameStartChars.toList
  private val ncNameChars = NCNameChars.toList
}
