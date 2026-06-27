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
package internal

import java.util.concurrent.atomic.AtomicReference

import cats.effect.MonadCancelThrow
import cats.syntax.all._
import org.apache.kafka.common.{Metric, MetricName}

/** Resolves Kafka's effective client id once per traced client.
  *
  * Kafka generates a client id when `client.id` is absent from the supplied settings. The generated value is exposed as
  * the `client-id` tag on the client's metrics, so reading the tag is more accurate than duplicating Kafka's
  * version-specific generation rules.
  */
private[otel4s] final class KafkaClientId[F[_]](
    configured: Option[String],
    metrics: () => F[Map[MetricName, Metric]]
)(implicit F: MonadCancelThrow[F]) {

  // The outer Option distinguishes unresolved from a resolved missing client id.
  private val cached =
    new AtomicReference[Option[Option[String]]](configured.map(Some(_)))

  def get: F[Option[String]] =
    F.unit.flatMap { _ =>
      cached.get() match {
        case Some(clientId) =>
          F.pure(clientId)

        case None =>
          metrics().attempt
            .map(_.toOption.flatMap(KafkaClientId.fromMetrics))
            .map { clientId =>
              if (cached.compareAndSet(None, Some(clientId))) clientId
              else cached.get().flatten
            }
      }
    }

}

private[otel4s] object KafkaClientId {

  private val ClientIdTag = "client-id"

  def apply[F[_]: MonadCancelThrow](
      configured: Option[String],
      metrics: => F[Map[MetricName, Metric]]
  ): KafkaClientId[F] =
    new KafkaClientId(configured, () => metrics)

  private[internal] def fromMetrics(metrics: Map[MetricName, Metric]): Option[String] =
    metrics.keysIterator
      .flatMap(metricName => Option(metricName.tags.get(ClientIdTag)))
      .find(_.nonEmpty)

}
