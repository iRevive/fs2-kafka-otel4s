/*
 * Copyright 2026 Typelevel
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

package fs2.kafka.otel4s.trace

import java.util.UUID

/** Typeclass describing how to derive a canonical Kafka message-key string for semantic conventions.
  *
  * Implementations should return `Some(...)` only when the key has an unambiguous canonical string representation
  * suitable for `messaging.kafka.message.key`. Returning `None` omits the attribute.
  */
trait KafkaMessageKey[-K] {

  def toMessageKey(value: K): Option[String]

  def contramap[K0](f: K0 => K): KafkaMessageKey[K0] =
    KafkaMessageKey.instance(value => toMessageKey(f(value)))

}

object KafkaMessageKey {

  def apply[K](implicit instance: KafkaMessageKey[K]): KafkaMessageKey[K] =
    instance

  def instance[K](f: K => Option[String]): KafkaMessageKey[K] =
    new KafkaMessageKey[K] {
      override def toMessageKey(value: K): Option[String] =
        f(value)
    }

  implicit val stringKafkaMessageKey: KafkaMessageKey[String] =
    instance(Option(_))

  implicit val intKafkaMessageKey: KafkaMessageKey[Int] =
    instance(value => Option(value).map(_.toString))

  implicit val longKafkaMessageKey: KafkaMessageKey[Long] =
    instance(value => Option(value).map(_.toString))

  implicit val uuidKafkaMessageKey: KafkaMessageKey[UUID] =
    instance(value => Option(value).map(_.toString))

  object Noop {
    implicit def noneKafkaMessageKey[K]: KafkaMessageKey[K] =
      KafkaMessageKey.instance(_ => None)
  }

}
