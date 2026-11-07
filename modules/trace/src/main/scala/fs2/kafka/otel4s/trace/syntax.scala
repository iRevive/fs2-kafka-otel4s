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

import cats.effect.Concurrent
import cats.syntax.functor._
import fs2.{Chunk, Stream}
import fs2.kafka.KafkaProducer
import fs2.kafka.consumer.KafkaConsumeChunk.CommitNow
import fs2.kafka.{CommittableConsumerRecord, ConsumerRecord, KafkaConsumer}
import org.typelevel.otel4s.context.ContextManager
import org.typelevel.otel4s.trace.TracerProvider

/** Stream-level syntax for binding tracing to raw consumer streams.
  */
trait KafkaConsumerStreamTracingSyntax {

  implicit final class KafkaConsumerStreamTracingOps[F[_], K, V](
      private val self: Stream[F, KafkaConsumer[F, K, V]]
  ) {

    /** Binds a [[KafkaTracer]] to each emitted raw consumer, yielding traced handles.
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

    /** Creates a library-managed [[KafkaTracer]] from `config`, then binds it to each emitted raw consumer.
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
        TP: TracerProvider[F],
        CM: ContextManager[F]
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

    /** Consumes from all assigned partitions concurrently, tracing each emitted chunk with `receive`, one batch
      * `process`, and owned `commit` spans.
      *
      * Is shorthand for:
      *
      * {{{
      * tracedConsumers.evalMap(_.consumeChunkTraced(chunkProcessor)).compile.onlyOrError
      * }}}
      *
      * Execution scope for one chunk, with all spans enabled and no ambient application span:
      * {{{
      * Input chunk: [m1, m2, m3]
      *
      *                          time -->
      *                     +--------------------------+--------+
      * handler             | handleChunk([m1,m2,m3])  |commit  |
      *                     |       called once        |offsets |
      *                     +--------------------------+--------+
      *
      * Span lifetime                                             Controlled by
      * --------------------------------------------------------  -------------------------
      * > receive           [=========================]           withReceiveSpanSetup
      * > process batch     [=========================]           withBatchProcessSpanSetup
      * > commit                                       [=======]  withCommitSpanSetup
      * }}}
      *
      * With the default process relationship, span rows are aligned because receive and process spans are independent
      * roots. Bars show timing; widths are illustrative. Returning `None` from a setup function removes only its span
      * row; handler execution and commit behavior are unchanged. Indentation is reserved for actual parent-child span
      * relationships.
      *
      * See the corresponding [[TracedKafkaConsumer]] method for span relationships and commit ownership.
      */
    def consumeChunkTraced(
        chunkProcessor: Chunk[ConsumerRecord[K, V]] => F[CommitNow]
    )(implicit F: Concurrent[F]): F[Nothing] =
      self.evalMap(_.consumeChunkTraced(chunkProcessor)).compile.onlyOrError

    /** Consumes from all assigned partitions concurrently, tracing each emitted chunk with `receive`, per-record
      * `process`, and owned `commit` spans.
      *
      * Is shorthand for:
      *
      * {{{
      * tracedConsumers.evalMap(_.consumeRecordTraced(recordProcessor)).compile.onlyOrError
      * }}}
      *
      * Execution scope for one chunk, with all spans enabled and no ambient application span:
      * {{{
      * Input chunk: [m1, m2, m3]
      *
      *                          time -->
      *                     +--------+--------+--------+--------+
      * handler             |handle  |handle  |handle  |commit  |
      *                     |  m1    |  m2    |  m3    |offsets |
      *                     +--------+--------+--------+--------+
      *
      * Span lifetime                                             Controlled by
      * --------------------------------------------------------  -------------------------
      * > receive           [=========================]           withReceiveSpanSetup
      * > process (m1)      [=======]                             withProcessSpanSetup
      * > process (m2)               [=======]                    withProcessSpanSetup
      * > process (m3)                        [=======]           withProcessSpanSetup
      * > commit                                       [=======]  withCommitSpanSetup
      * }}}
      *
      * With the default process relationship, span rows are aligned because receive and process spans are independent
      * roots. Bars show timing; widths are illustrative. Returning `None` from a setup function removes only its span
      * row; handler execution and commit behavior are unchanged. Indentation is reserved for actual parent-child span
      * relationships.
      *
      * See the corresponding [[TracedKafkaConsumer]] method for span relationships and commit ownership.
      */
    def consumeRecordTraced[A](
        recordProcessor: ConsumerRecord[K, V] => F[A]
    )(implicit F: Concurrent[F]): F[Nothing] =
      self.evalMap(_.consumeRecordTraced(recordProcessor)).compile.onlyOrError

    /** Convenience stream for the common traced-consumption shape: a chunk-level `receive` span around delivery, plus a
      * per-record `process` span for each record in that chunk.
      *
      * Is shorthand for:
      *
      * {{{
      * tracedConsumers.flatMap(_.recordsTraced(f))
      * }}}
      *
      * Execution scope for one chunk, with all spans enabled and no ambient application span:
      * {{{
      * Input chunk: [m1, m2, m3]
      *
      *                          time -->
      *                     +--------+--------+--------+--------+
      * work                |deliver |handle  |handle  |handle  |
      *                     | chunk  |  m1    |  m2    |  m3    |
      *                     +--------+--------+--------+--------+
      *
      * Span lifetime                                             Controlled by
      * --------------------------------------------------------  -------------------------
      * > receive           [=======]                             withReceiveSpanSetup
      * > process (m1)               [=======]                    withProcessSpanSetup
      * > process (m2)                        [=======]           withProcessSpanSetup
      * > process (m3)                                 [=======]  withProcessSpanSetup
      *
      * Results are emitted after each handler; no automatic commit.
      * }}}
      *
      * With the default process relationship, span rows are aligned because receive and process spans are independent
      * roots. Bars show timing; widths are illustrative. Returning `None` from a setup function removes only its span
      * row; handler execution and commit behavior are unchanged. Indentation is reserved for actual parent-child span
      * relationships.
      *
      * See the corresponding [[TracedKafkaConsumer]] method for span relationships and commit ownership.
      */
    def recordsTraced[A](
        f: CommittableConsumerRecord[F, K, V] => F[A]
    ): Stream[F, A] =
      self.flatMap(_.recordsTraced(f))

  }

}

trait KafkaProducerStreamTracingSyntax {

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
        TP: TracerProvider[F],
        CM: ContextManager[F]
    ): Stream[F, TracedKafkaProducer[F, K, V]] =
      Stream
        .eval(KafkaTracer.create[F](config))
        .flatMap(kafkaTracer => self.map(kafkaTracer.producer(_)))

  }

}

trait KafkaProducerTracingSyntax {

  implicit final class KafkaProducerTracingOps[F[_], K, V](
      private val self: KafkaProducer.WithSettings[F, K, V]
  ) {

    /** Binds a [[KafkaTracer]] to the raw producer, yielding traced variant.
      *
      * Is shorthand for:
      *
      * {{{
      * kafkaTracer.producer(producer)
      * }}}
      */
    def traced(
        kafkaTracer: KafkaTracer[F]
    )(implicit
        ev: KafkaMessageKey[K]
    ): TracedKafkaProducer[F, K, V] =
      kafkaTracer.producer(self)

    /** Creates a library-managed [[KafkaTracer]] from `config`, then binds it to the raw producer.
      *
      * Is shorthand for:
      *
      * {{{
      * KafkaTracer.create[F](config).evalMap(kafkaTracer => kafkaTracer.producer(producer))
      * }}}
      */
    def traced(
        config: KafkaTracer.Config
    )(implicit
        ev: KafkaMessageKey[K],
        F: Concurrent[F],
        TP: TracerProvider[F],
        CM: ContextManager[F]
    ): F[TracedKafkaProducer[F, K, V]] =
      KafkaTracer
        .create[F](config)
        .map(kafkaTracer => kafkaTracer.producer(self))

  }

}

/** Syntax imports for binding traced producers and consumers and consuming traced streams.
  */
object syntax
    extends KafkaConsumerStreamTracingSyntax
    with TracedKafkaConsumerStreamTracingSyntax
    with KafkaProducerStreamTracingSyntax
    with KafkaProducerTracingSyntax
