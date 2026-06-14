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

import cats.effect.{IO, Resource}
import fs2.kafka.{KafkaConsumer, KafkaProducer}
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.propagation.TextMapPropagator
import io.opentelemetry.sdk.trace.SdkTracerProviderBuilder
import io.opentelemetry.sdk.trace.data.SpanData
import org.typelevel.otel4s.oteljava.testkit.OtelJavaTestkit
import org.typelevel.otel4s.trace.{Tracer, TracerProvider}

final class KafkaTracerTestkit(testkit: OtelJavaTestkit[IO])(implicit
    val tracerProvider: TracerProvider[IO],
    val appTracer: Tracer[IO]
) {

  def tracedProducer[K: KafkaMessageKey, V](
      producer: KafkaProducer.WithSettings[IO, K, V],
      config: KafkaTracer.Config = KafkaTracer.Config.default
  ): IO[TracedKafkaProducer[IO, K, V]] =
    KafkaTracer.create[IO](config).map(_.producer(producer))

  def tracedConsumer[K: KafkaMessageKey, V](
      consumer: KafkaConsumer[IO, K, V],
      config: KafkaTracer.Config = KafkaTracer.Config.default
  ): IO[TracedKafkaConsumer[IO, K, V]] =
    KafkaTracer.create[IO](config).map(_.consumer(consumer))

  def finishedSpans: IO[List[SpanData]] =
    testkit.finishedSpans

}

object KafkaTracerTestkit {

  def create(
      appTracerName: String = "fs2.kafka.otel4s.tests",
      propagators: Seq[TextMapPropagator] = Seq(W3CTraceContextPropagator.getInstance()),
      tracerProviderCustomizer: SdkTracerProviderBuilder => SdkTracerProviderBuilder = identity
  ): Resource[IO, KafkaTracerTestkit] =
    OtelJavaTestkit
      .inMemory[IO](
        _.addTextMapPropagators(propagators *)
          .addTracerProviderCustomizer(tracerProviderCustomizer)
      )
      .evalMap { testkit =>
        for {
          tracer <- testkit.tracerProvider.get(appTracerName)
        } yield new KafkaTracerTestkit(testkit)(testkit.tracerProvider, tracer)
      }

}
