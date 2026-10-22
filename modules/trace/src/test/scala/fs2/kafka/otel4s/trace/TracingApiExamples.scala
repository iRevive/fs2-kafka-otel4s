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

package fs2.kafka.otel4s.trace.examples

import cats.Parallel
import cats.effect.{Async, IO}
import fs2.kafka.KafkaConsumer
import fs2.kafka.KafkaConsumer._
import fs2.kafka.{ConsumerRecord, ConsumerSettings}
import fs2.kafka.otel4s.trace.{ConsumerMetadata, KafkaMessageKey, KafkaTracer}
import org.typelevel.otel4s.trace.TracerProvider

/** Compile-checked examples of the intended consumer API imports and call shapes. */
private[trace] object TracingApiExamples {

  def highLevelWithConfig[F[_]: Async: Parallel: TracerProvider, K: KafkaMessageKey, V](
      settings: ConsumerSettings[F, K, V],
      config: KafkaTracer.Config,
      handle: ConsumerRecord[K, V] => F[Unit]
  ): F[Nothing] = {
    import fs2.kafka.otel4s.trace.syntax._

    KafkaConsumer
      .stream[F, K, V](settings)
      .subscribeTo("orders")
      .traced(config)
      .consumeRecordTraced(handle)
  }

  def highLevelWithTracer[F[_]: Async, K: KafkaMessageKey, V](
      settings: ConsumerSettings[F, K, V],
      tracer: KafkaTracer[F],
      handle: ConsumerRecord[K, V] => F[Unit]
  ): F[Nothing] = {
    import fs2.kafka.otel4s.trace.syntax._

    KafkaConsumer
      .stream[F, K, V](settings)
      .subscribeTo("orders")
      .traced(tracer)
      .consumeRecordTraced(handle)
  }

  // Deliberately has no trace.syntax import: low-level operations are ordinary methods on the selected handle.
  def lowLevel(record: ConsumerRecord[String, String], tracer: KafkaTracer[IO]): IO[Unit] = {
    val spans = tracer.consumerSpans(ConsumerMetadata(Some("orders-client"), Some("orders-group")))
    spans.process(record)(IO.unit)
  }

}
