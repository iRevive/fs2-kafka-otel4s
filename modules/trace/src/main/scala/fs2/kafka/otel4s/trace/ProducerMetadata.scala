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

import cats.effect.MonadCancelThrow
import cats.Applicative
import cats.syntax.functor._
import fs2.kafka.KafkaProducer
import fs2.kafka.otel4s.trace.internal.KafkaClientId
import org.apache.kafka.clients.CommonClientConfigs

/** Client identity used by producer tracing operations.
  *
  * An explicit value contains only metadata and does not retain a Kafka producer.
  * [[ProducerMetadata.Provider.fromProducer]] resolves the effective client id, including Kafka-generated ids exposed
  * through metrics. If metrics lookup fails, the provider caches a missing client id and does not refresh it.
  */
sealed trait ProducerMetadata {
  def clientId: Option[String]
}

object ProducerMetadata {

  /** Resolves and caches metadata for a bound client, or returns an explicit snapshot. A provider created with
    * [[ProducerMetadata.Provider.fromProducer]] retains the producer through its metrics resolver for the provider's
    * lifetime. Concurrent initial `get` calls may perform multiple metrics lookups; after a result is cached, later
    * calls reuse it, including a missing result after lookup failure. Use [[ProducerMetadata.Provider.constant]] for a
    * value-only snapshot that retains no client.
    */
  trait Provider[F[_]] {
    def get: F[ProducerMetadata]
  }

  object Provider {

    def constant[F[_]: Applicative](metadata: ProducerMetadata): Provider[F] =
      new Provider[F] {
        override def get: F[ProducerMetadata] = Applicative[F].pure(metadata)
      }

    /** Creates a client-bound provider. Generated client ids are resolved from metrics and cached for this provider's
      * lifetime. Concurrent initial lookups may run more than once; a cached metrics failure is represented as a
      * missing client id and is not refreshed.
      */
    def fromProducer[F[_]: MonadCancelThrow, K, V](producer: KafkaProducer.WithSettings[F, K, V]): Provider[F] = {
      val clientId = KafkaClientId[F](
        producer.settings.properties.get(CommonClientConfigs.CLIENT_ID_CONFIG),
        producer.metrics
      )

      new Provider[F] {
        override def get: F[ProducerMetadata] =
          clientId.get.map(ProducerMetadata(_))
      }
    }

  }

  def apply(clientId: Option[String]): ProducerMetadata =
    Impl(clientId)

  private final case class Impl(clientId: Option[String]) extends ProducerMetadata

}
