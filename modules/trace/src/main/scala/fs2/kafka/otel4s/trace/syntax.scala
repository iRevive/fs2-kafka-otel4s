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
import fs2.{Chunk, Stream}
import fs2.kafka.consumer.KafkaConsumeChunk.CommitNow
import fs2.kafka.{CommittableConsumerRecord, ConsumerRecord, KafkaConsumer}
import org.typelevel.otel4s.trace.TracerProvider

/** Local syntax for explicitly traced consumer chunk and record boundaries.
  */
trait ConsumerTracingSyntax {

  implicit final class ConsumerRecordChunkTracingOps[K, V](
      private val records: Chunk[ConsumerRecord[K, V]]
  ) {

    /** Convenience syntax for [[TracedKafkaConsumer.receive]] on a plain `Chunk[ConsumerRecord[...]]`.
      *
      * Is shorthand for:
      *
      * {{{
      * tracedConsumer.receive(records)(fa)
      * }}}
      */
    def traceReceive[F[_], A](fa: F[A])(implicit
        tracedConsumer: TracedKafkaConsumer[F, K, V]
    ): F[A] =
      tracedConsumer.receive(records)(fa)

  }

  implicit final class ConsumerChunkTracingOps[F[_], K, V](
      private val records: Chunk[CommittableConsumerRecord[F, K, V]]
  ) {

    /** Convenience syntax for [[TracedKafkaConsumer.receiveCommittable]] on a
      * `Chunk[CommittableConsumerRecord[...]]`.
      *
      * Is shorthand for:
      *
      * {{{
      * tracedConsumer.receiveCommittable(records)(fa)
      * }}}
      */
    def traceReceive[A](fa: F[A])(implicit
        tracedConsumer: TracedKafkaConsumer[F, K, V]
    ): F[A] =
      tracedConsumer.receiveCommittable(records)(fa)

  }

  implicit final class ConsumerRecordTracingOps[K, V](
      private val record: ConsumerRecord[K, V]
  ) {

    /** Convenience syntax for [[TracedKafkaConsumer.process]] on a plain [[ConsumerRecord]].
      *
      * Is shorthand for:
      *
      * {{{
      * tracedConsumer.process(record)(fa)
      * }}}
      */
    def traceProcess[F[_], A](fa: F[A])(implicit
        tracedConsumer: TracedKafkaConsumer[F, K, V]
    ): F[A] =
      tracedConsumer.process(record)(fa)

  }

  implicit final class CommittableConsumerRecordTracingOps[F[_], K, V](
      private val record: CommittableConsumerRecord[F, K, V]
  ) {

    /** Convenience syntax for [[TracedKafkaConsumer.process]] on a [[CommittableConsumerRecord]].
      *
      * Is shorthand for:
      *
      * {{{
      * tracedConsumer.process(record)(fa)
      * }}}
      */
    def traceProcess[A](fa: F[A])(implicit
        tracedConsumer: TracedKafkaConsumer[F, K, V]
    ): F[A] =
      tracedConsumer.process(record)(fa)

  }

}

/** Stream-level syntax for binding tracing to raw consumer streams.
  */
trait KafkaConsumerStreamTracingSyntax {

  implicit final class KafkaConsumerStreamTracingOps[F[_], K, V](
      private val self: Stream[F, KafkaConsumer[F, K, V]]
  ) {

    /** Convenience syntax that binds a [[KafkaTracer]] to each emitted raw consumer, yielding traced handles.
      *
      * Is shorthand for:
      *
      * {{{
      * consumers.map(kafkaTracer.consumer(_))
      * }}}
      */
    def traced(
        kafkaTracer: KafkaTracer[F]
    )(implicit
        ev: KafkaMessageKey[K]
    ): Stream[F, TracedKafkaConsumer[F, K, V]] =
      self.map(kafkaTracer.consumer(_))

    /** Convenience syntax that creates a library-managed [[KafkaTracer]] from `config`, then binds it to each emitted
      * raw consumer.
      *
      * The tracer is created once per stream evaluation, not once per emitted consumer.
      *
      * Is shorthand for:
      *
      * {{{
      * Stream.eval(KafkaTracer.create[F](config)).flatMap(kafkaTracer => consumers.map(kafkaTracer.consumer(_)))
      * }}}
      */
    def traced(
        config: KafkaTracer.Config
    )(implicit
        ev: KafkaMessageKey[K],
        F: Concurrent[F],
        P: Parallel[F],
        TP: TracerProvider[F]
    ): Stream[F, TracedKafkaConsumer[F, K, V]] =
      Stream
        .eval(KafkaTracer.create[F](config))
        .flatMap(kafkaTracer => self.map(kafkaTracer.consumer(_)))

  }

}

/** Stream-level syntax for traced consumer streams.
  */
trait TracedKafkaConsumerStreamTracingSyntax {

  implicit final class TracedKafkaConsumerStreamTracingOps[F[_], K, V](
      private val self: Stream[F, TracedKafkaConsumer[F, K, V]]
  ) {

    /** Convenience syntax for consuming a stream of traced consumers with [[TracedKafkaConsumer.consumeChunk]].
      *
      * Is shorthand for:
      *
      * {{{
      * tracedConsumers.evalMap(_.consumeChunk(processor)).compile.onlyOrError
      * }}}
      */
    def consumeChunk(
        processor: Chunk[ConsumerRecord[K, V]] => F[CommitNow]
    )(implicit F: Concurrent[F]): F[Nothing] =
      self.evalMap(_.consumeChunk(processor)).compile.onlyOrError

    /** Convenience syntax for flattening a stream of traced consumers via [[TracedKafkaConsumer.recordsWithProcess]].
      *
      * Is shorthand for:
      *
      * {{{
      * tracedConsumers.flatMap(_.recordsWithProcess(f))
      * }}}
      */
    def recordsWithProcess[A](
        f: CommittableConsumerRecord[F, K, V] => F[A]
    ): Stream[F, A] =
      self.flatMap(_.recordsWithProcess(f))

  }

}

/** Syntax imports for traced consumer chunk/record helpers and traced consumer stream helpers.
  */
object syntax
    extends ConsumerTracingSyntax
    with KafkaConsumerStreamTracingSyntax
    with TracedKafkaConsumerStreamTracingSyntax
