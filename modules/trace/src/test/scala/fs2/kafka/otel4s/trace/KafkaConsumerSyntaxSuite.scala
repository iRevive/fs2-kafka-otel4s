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

import org.typelevel.otel4s.context.ContextManager
import cats.effect.{IO, Ref}
import fs2.{Chunk, Stream}
import fs2.kafka._
import fs2.kafka.consumer.KafkaConsumeChunk.CommitNow
import fs2.kafka.otel4s.trace.syntax._

final class KafkaConsumerSyntaxSuite extends KafkaTracingTestSupport {

  test("Stream[KafkaConsumer].traced binds tracing and yields traced consumers") {
    for {
      expected <- ConsumerSyntaxProbe.create()
      underlying = StubKafkaConsumer.metadataOnly[String, String]()
      tracer = new KafkaTracer[IO] {
        override def producer[K: KafkaMessageKey, V](
            producer: KafkaProducer.WithSettings[IO, K, V]
        ): TracedKafkaProducer[IO, K, V] =
          throw new AssertionError("unexpected producer")

        override def consumer[K: KafkaMessageKey, V](
            consumer: KafkaConsumer[IO, K, V]
        ): TracedKafkaConsumer[IO, K, V] =
          expected.asInstanceOf[TracedKafkaConsumer[IO, K, V]]

        override def producerSpans(metadata: ProducerMetadata.Provider[IO]): ProducerSpans[IO] =
          KafkaTracer.noop[IO].producerSpans(metadata)

        override def consumerSpans(metadata: ConsumerMetadata.Provider[IO]): ConsumerSpans[IO] =
          throw new AssertionError("unexpected consumer spans")
      }
      result <- Stream
        .emit(underlying)
        .traced(tracer)
        .compile
        .toList
    } yield assertEquals(result, List(expected))
  }

  test("Stream[KafkaConsumer].traced(config) creates a tracer and yields traced consumers") {
    KafkaTracerTestkit.create().use { testkit =>
      val underlying = StubKafkaConsumer.metadataOnly[String, String]()

      implicit val tracerProvider = testkit.tracerProvider
      implicit val contextManager: ContextManager[IO] = testkit.contextManager

      Stream
        .emit(underlying)
        .traced(KafkaTracer.Config.default)
        .compile
        .toList
        .map { result =>
          assertEquals(result.length, 1)
          assert(result.head.underlying eq underlying)
        }
    }
  }

  test("Stream[TracedKafkaConsumer].consumeChunkTraced(chunk) delegates to traced consumeChunkTraced") {
    for {
      probe <- ConsumerSyntaxProbe.create()
      _ <- Stream
        .emit(probe)
        .consumeChunkTraced(_ => IO.pure(CommitNow))
        .attempt
      seen <- probe.consumeChunkTracedChunkCalled.get
    } yield assertEquals(seen, true)
  }

  test("Stream[TracedKafkaConsumer].consumeRecordTraced delegates to traced consumeRecordTraced") {
    for {
      probe <- ConsumerSyntaxProbe.create()
      _ <- Stream
        .emit(probe)
        .consumeRecordTraced(_ => IO.unit)
        .attempt
      seen <- probe.consumeRecordTracedCalled.get
    } yield assertEquals(seen, true)
  }

  test("Stream[TracedKafkaConsumer].recordsTraced delegates to traced recordsTraced") {
    val record = StubKafkaConsumer.committableRecord(ConsumerRecord("topic", 0, 1L, "k", "v"))

    for {
      probe <- ConsumerSyntaxProbe.create(recordsTracedResult = Stream.emit(record))
      result <- Stream
        .emit(probe)
        .recordsTraced(IO.pure)
        .compile
        .toList
      seen <- probe.recordsTracedCalled.get
    } yield {
      assertEquals(seen, true)
      assertEquals(result, List(record))
    }
  }

  final class ConsumerSyntaxProbe(
      override val underlying: KafkaConsumer[IO, String, String],
      val consumeChunkTracedChunkCalled: Ref[IO, Boolean],
      val consumeRecordTracedCalled: Ref[IO, Boolean],
      val recordsTracedCalled: Ref[IO, Boolean],
      recordsTracedResult: Stream[IO, CommittableConsumerRecord[IO, String, String]] = Stream.empty
  ) extends TracedKafkaConsumer[IO, String, String] {

    override val spans: ConsumerSpans[IO] = KafkaTracer
      .noop[IO]
      .consumerSpans(
        ConsumerMetadata.Provider.constant[IO](ConsumerMetadata(None, None))
      )

    override def consumeChunkTraced(
        processor: Chunk[ConsumerRecord[String, String]] => IO[CommitNow]
    ): IO[Nothing] =
      consumeChunkTracedChunkCalled.set(true) *> IO.raiseError(new RuntimeException("stop"))

    override def consumeRecordTraced[A](
        processor: ConsumerRecord[String, String] => IO[A]
    ): IO[Nothing] =
      consumeRecordTracedCalled.set(true) *> IO.raiseError(new RuntimeException("stop"))

    override def recordsTraced[A](
        f: CommittableConsumerRecord[IO, String, String] => IO[A]
    ): Stream[IO, A] =
      Stream.eval(recordsTracedCalled.set(true)).flatMap(_ => recordsTracedResult.evalMap(f))

  }

  object ConsumerSyntaxProbe {
    def create(
        underlying: KafkaConsumer[IO, String, String] = StubKafkaConsumer.metadataOnly(),
        recordsTracedResult: Stream[IO, CommittableConsumerRecord[IO, String, String]] = Stream.empty
    ): IO[ConsumerSyntaxProbe] =
      for {
        consumeChunkTracedChunkCalled <- Ref[IO].of(false)
        consumeRecordTracedCalled <- Ref[IO].of(false)
        recordsTracedCalled <- Ref[IO].of(false)
      } yield new ConsumerSyntaxProbe(
        underlying,
        consumeChunkTracedChunkCalled,
        consumeRecordTracedCalled,
        recordsTracedCalled,
        recordsTracedResult
      )
  }

}
