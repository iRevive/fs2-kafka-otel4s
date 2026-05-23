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

import cats.effect.{IO, Ref}
import fs2.{Chunk, Stream}
import fs2.kafka._
import fs2.kafka.consumer.KafkaConsumeChunk.CommitNow
import fs2.kafka.otel4s.trace.syntax._

final class KafkaConsumerSyntaxSuite extends KafkaTracingTestSupport {

  test("Chunk[ConsumerRecord].traceReceive delegates to receive") {
    for {
      probe <- ConsumerSyntaxProbe.create()
      record = ConsumerRecord("topic", 0, 42L, "key", "value")
      chunk = Chunk.singleton(record)
      result <- chunk.traceReceive(IO.pure("ok"))(probe)
      seen <- probe.receivedChunk.get
    } yield {
      assertEquals(seen, Some(chunk))
      assertEquals(result, "ok")
    }
  }

  test("Chunk[CommittableConsumerRecord].traceReceive delegates to receiveCommittable") {
    for {
      probe <- ConsumerSyntaxProbe.create()
      record = StubKafkaConsumer.committableRecord(ConsumerRecord("topic", 0, 42L, "key", "value"))
      chunk = Chunk.singleton(record)
      result <- chunk.traceReceive(IO.pure("ok"))(probe)
      seen <- probe.receivedCommittableChunk.get
    } yield {
      assertEquals(seen, Some(chunk))
      assertEquals(result, "ok")
    }
  }

  test("ConsumerRecord.traceProcess delegates to process") {
    for {
      probe <- ConsumerSyntaxProbe.create()
      record = ConsumerRecord("topic", 0, 42L, "key", "value")
      result <- record.traceProcess(IO.pure("ok"))(probe)
      seen <- probe.processedRecord.get
    } yield {
      assertEquals(seen, Some(record))
      assertEquals(result, "ok")
    }
  }

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

  test("Stream[TracedKafkaConsumer].consumeChunk delegates to traced consumeChunk") {
    for {
      probe <- ConsumerSyntaxProbe.create()
      _ <- Stream
             .emit(probe)
             .consumeChunk(_ => IO.pure(CommitNow))
             .attempt
      seen <- probe.consumeChunkCalled.get
    } yield assertEquals(seen, true)
  }

  test("Stream[TracedKafkaConsumer].recordsWithProcess delegates to traced recordsWithProcess") {
    val record = StubKafkaConsumer.committableRecord(ConsumerRecord("topic", 0, 1L, "k", "v"))

    for {
      probe <- ConsumerSyntaxProbe.create(recordsWithProcessResult = Stream.emit(record))
      result <- Stream
                  .emit(probe)
                  .recordsWithProcess(IO.pure)
                  .compile
                  .toList
      seen <- probe.recordsWithProcessCalled.get
    } yield {
      assertEquals(seen, true)
      assertEquals(result, List(record))
    }
  }

  final class ConsumerSyntaxProbe(
      override val underlying: KafkaConsumer[IO, String, String],
      val receivedChunk: Ref[IO, Option[Chunk[ConsumerRecord[String, String]]]],
      val receivedCommittableChunk: Ref[IO, Option[Chunk[CommittableConsumerRecord[IO, String, String]]]],
      val processedRecord: Ref[IO, Option[ConsumerRecord[String, String]]],
      val processedCommittable: Ref[IO, Option[CommittableConsumerRecord[IO, String, String]]],
      val consumeChunkCalled: Ref[IO, Boolean],
      val recordsWithProcessCalled: Ref[IO, Boolean],
      recordsWithProcessResult: Stream[IO, CommittableConsumerRecord[IO, String, String]] = Stream.empty
  ) extends TracedKafkaConsumer[IO, String, String] {

    override def records: Stream[IO, CommittableConsumerRecord[IO, String, String]] =
      Stream.empty

    override def partitionedRecords: Stream[IO, Stream[IO, CommittableConsumerRecord[IO, String, String]]] =
      Stream.empty

    override def partitionedStream: Stream[IO, Stream[IO, CommittableConsumerRecord[IO, String, String]]] =
      Stream.empty

    override def consumeChunk(
        processor: Chunk[ConsumerRecord[String, String]] => IO[CommitNow]
    ): IO[Nothing] =
      consumeChunkCalled.set(true) *> IO.raiseError(new RuntimeException("stop"))

    override def receive[A](
        records: Chunk[ConsumerRecord[String, String]]
    )(fa: IO[A]): IO[A] =
      receivedChunk.set(Some(records)) *> fa

    override def receiveCommittable[A](
        records: Chunk[CommittableConsumerRecord[IO, String, String]]
    )(fa: IO[A]): IO[A] =
      receivedCommittableChunk.set(Some(records)) *> fa

    override def process[A](record: ConsumerRecord[String, String])(fa: IO[A]): IO[A] =
      processedRecord.set(Some(record)) *> fa

    override def process[A](
        record: CommittableConsumerRecord[IO, String, String]
    )(fa: IO[A]): IO[A] =
      processedCommittable.set(Some(record)) *> fa

    override def recordsWithProcess[A](
        f: CommittableConsumerRecord[IO, String, String] => IO[A]
    ): Stream[IO, A] =
      Stream.eval(recordsWithProcessCalled.set(true)).flatMap(_ => recordsWithProcessResult.evalMap(f))

  }

  object ConsumerSyntaxProbe {
    def create(
        recordsWithProcessResult: Stream[IO, CommittableConsumerRecord[IO, String, String]] = Stream.empty
    ): IO[ConsumerSyntaxProbe] =
      for {
        receiveChunk <- Ref[IO].of(Option.empty[Chunk[ConsumerRecord[String, String]]])
        receiveCommittableChunk <- Ref[IO].of(
                                     Option.empty[Chunk[CommittableConsumerRecord[IO, String, String]]]
                                   )
        processedRecord <- Ref[IO].of(Option.empty[ConsumerRecord[String, String]])
        processedCommittable <- Ref[IO].of(
                                  Option.empty[CommittableConsumerRecord[IO, String, String]]
                                )
        consumeChunkCalled <- Ref[IO].of(false)
        recordsWithProcessCalled <- Ref[IO].of(false)
      } yield new ConsumerSyntaxProbe(
        StubKafkaConsumer.metadataOnly(),
        receiveChunk,
        receiveCommittableChunk,
        processedRecord,
        processedCommittable,
        consumeChunkCalled,
        recordsWithProcessCalled,
        recordsWithProcessResult
      )
  }

}
