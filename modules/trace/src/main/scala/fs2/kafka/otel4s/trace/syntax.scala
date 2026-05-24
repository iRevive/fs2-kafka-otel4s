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

import cats.Parallel
import cats.effect.Concurrent
import fs2.Stream
import fs2.kafka.KafkaProducer
import org.typelevel.otel4s.trace.TracerProvider

/** Syntax imports for traced producer stream binding.
  */
object syntax {

  implicit final class KafkaProducerStreamTracingOps[F[_], K, V](
      private val self: Stream[F, KafkaProducer.WithSettings[F, K, V]]
  ) {

    /** Binds a [[KafkaTracer]] to each emitted raw producer, yielding traced handles.
      *
      * Is shorthand for:
      *
      * {{{
      * producers.map(kafkaTracer.producer(_))
      * }}}
      */
    def traced(
        kafkaTracer: KafkaTracer[F]
    )(implicit
        ev: KafkaMessageKey[K]
    ): Stream[F, TracedKafkaProducer[F, K, V]] =
      self.map(kafkaTracer.producer(_))

    /** Creates a library-managed [[KafkaTracer]] from `config`, then binds it to each emitted raw producer.
      *
      * The tracer is created once per stream evaluation, not once per emitted producer.
      *
      * Is shorthand for:
      *
      * {{{
      * Stream.eval(KafkaTracer.create[F](config)).flatMap(kafkaTracer => producers.map(kafkaTracer.producer(_)))
      * }}}
      */
    def traced(
        config: KafkaTracer.Config
    )(implicit
        ev: KafkaMessageKey[K],
        F: Concurrent[F],
        P: Parallel[F],
        TP: TracerProvider[F]
    ): Stream[F, TracedKafkaProducer[F, K, V]] =
      Stream
        .eval(KafkaTracer.create[F](config))
        .flatMap(kafkaTracer => self.map(kafkaTracer.producer(_)))

  }

}
