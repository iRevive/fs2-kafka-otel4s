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

import cats.Applicative
import cats.effect.MonadCancelThrow
import cats.syntax.functor._
import fs2.kafka.KafkaConsumer
import fs2.kafka.otel4s.trace.internal.KafkaClientId
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig

/** Client identity used by consumer tracing operations.
  *
  * An explicit value contains only metadata and does not retain a Kafka consumer.
  * [[ConsumerMetadata.Provider.fromConsumer]] resolves the effective client id, including Kafka-generated ids exposed
  * through metrics, and snapshots the consumer group id. If metrics lookup fails, the client id is `None`; the provider
  * caches that result and does not refresh it.
  */
sealed trait ConsumerMetadata {
  def clientId: Option[String]
  def groupId: Option[String]
}

object ConsumerMetadata {

  /** Resolves and caches metadata for a bound client, or returns an explicit snapshot. A provider created with
    * [[ConsumerMetadata.Provider.fromConsumer]] retains the consumer through its metrics resolver for the provider's
    * lifetime. Concurrent initial `get` calls may perform multiple metrics lookups; after a result is cached, later
    * calls reuse it, including a missing result after lookup failure. Use [[ConsumerMetadata.Provider.constant]] for a
    * value-only snapshot that retains no client.
    */
  trait Provider[F[_]] {
    def get: F[ConsumerMetadata]
  }

  object Provider {

    def constant[F[_]: Applicative](metadata: ConsumerMetadata): Provider[F] =
      new Provider[F] {
        override def get: F[ConsumerMetadata] = Applicative[F].pure(metadata)
      }

    /** Creates a client-bound provider. Generated client ids are resolved from metrics and cached for this provider's
      * lifetime. Concurrent initial lookups may run more than once; a cached metrics failure is represented as a
      * missing client id and is not refreshed.
      */
    def fromConsumer[F[_]: MonadCancelThrow, K, V](consumer: KafkaConsumer[F, K, V]): Provider[F] = {
      val clientId = KafkaClientId[F](
        consumer.settings.properties.get(CommonClientConfigs.CLIENT_ID_CONFIG),
        consumer.metrics
      )
      val groupId = consumer.settings.properties.get(ConsumerConfig.GROUP_ID_CONFIG)

      new Provider[F] {
        override def get: F[ConsumerMetadata] =
          clientId.get.map(ConsumerMetadata(_, groupId))
      }
    }

  }

  def apply(clientId: Option[String], groupId: Option[String]): ConsumerMetadata =
    Impl(clientId, groupId)

  private final case class Impl(clientId: Option[String], groupId: Option[String]) extends ConsumerMetadata

}
