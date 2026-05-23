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

import scala.collection.immutable.SortedSet
import scala.concurrent.duration.FiniteDuration
import scala.util.matching.Regex

import cats.{Foldable, Reducible}
import cats.data.NonEmptySet
import cats.effect.IO
import fs2.Stream
import fs2.kafka._
import org.apache.kafka.clients.consumer.{ConsumerGroupMetadata, OffsetAndMetadata, OffsetAndTimestamp}
import org.apache.kafka.common.{Metric, MetricName, PartitionInfo, TopicPartition}

class StubKafkaConsumer[K, V](
    override val settings: ConsumerSettings[IO, K, V]
) extends KafkaConsumer[IO, K, V] {

  override def assignment: IO[SortedSet[TopicPartition]] = unsupported
  override def assignmentStream: Stream[IO, SortedSet[TopicPartition]] = Stream.eval(unsupported)
  override def assign(partitions: NonEmptySet[TopicPartition]): IO[Unit] = unsupported
  override def assign(topic: String): IO[Unit] = unsupported
  override def commitAsync(offsets: Map[TopicPartition, OffsetAndMetadata]): IO[Unit] = unsupported
  override def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): IO[Unit] = unsupported
  override def stream: Stream[IO, CommittableConsumerRecord[IO, K, V]] = Stream.eval(unsupported)
  override def partitionedStream: Stream[IO, Stream[IO, CommittableConsumerRecord[IO, K, V]]] =
    Stream.eval(unsupported)
  override def partitionsMapStream
      : Stream[IO, Map[TopicPartition, Stream[IO, CommittableConsumerRecord[IO, K, V]]]] =
    Stream.eval(unsupported)
  override def stopConsuming: IO[Unit] = unsupported
  override def terminate: IO[Unit] = unsupported
  override def awaitTermination: IO[Unit] = unsupported
  override def metrics: IO[Map[MetricName, Metric]] = unsupported
  override def committed(
      partitions: Set[TopicPartition]
  ): IO[Map[TopicPartition, OffsetAndMetadata]] = unsupported
  override def committed(
      partitions: Set[TopicPartition],
      timeout: FiniteDuration
  ): IO[Map[TopicPartition, OffsetAndMetadata]] = unsupported
  override def seek(partition: TopicPartition, offset: Long): IO[Unit] = unsupported
  override def seekToBeginning[G[_]: Foldable](partitions: G[TopicPartition]): IO[Unit] = unsupported
  override def seekToEnd[G[_]: Foldable](partitions: G[TopicPartition]): IO[Unit] = unsupported
  override def position(partition: TopicPartition): IO[Long] = unsupported
  override def position(partition: TopicPartition, timeout: FiniteDuration): IO[Long] = unsupported
  override def subscribe[G[_]: Reducible](topics: G[String]): IO[Unit] = unsupported
  override def subscribe(regex: Regex): IO[Unit] = unsupported
  override def unsubscribe: IO[Unit] = unsupported
  override def groupMetadata: IO[ConsumerGroupMetadata] = unsupported
  override def partitionsFor(topic: String): IO[List[PartitionInfo]] = unsupported
  override def partitionsFor(topic: String, timeout: FiniteDuration): IO[List[PartitionInfo]] = unsupported
  override def beginningOffsets(partitions: Set[TopicPartition]): IO[Map[TopicPartition, Long]] = unsupported
  override def beginningOffsets(
      partitions: Set[TopicPartition],
      timeout: FiniteDuration
  ): IO[Map[TopicPartition, Long]] = unsupported
  override def endOffsets(partitions: Set[TopicPartition]): IO[Map[TopicPartition, Long]] = unsupported
  override def endOffsets(
      partitions: Set[TopicPartition],
      timeout: FiniteDuration
  ): IO[Map[TopicPartition, Long]] = unsupported
  override def listTopics: IO[Map[String, List[PartitionInfo]]] = unsupported
  override def listTopics(timeout: FiniteDuration): IO[Map[String, List[PartitionInfo]]] = unsupported
  override def offsetsForTimes(
      timestampsToSearch: Map[TopicPartition, Long]
  ): IO[Map[TopicPartition, Option[OffsetAndTimestamp]]] = unsupported
  override def offsetsForTimes(
      timestampsToSearch: Map[TopicPartition, Long],
      timeout: FiniteDuration
  ): IO[Map[TopicPartition, Option[OffsetAndTimestamp]]] = unsupported

  private def unsupported[A]: IO[A] =
    IO.raiseError(new AssertionError("StubKafkaConsumer method should not be called"))

}

object StubKafkaConsumer {

  def metadataOnly[K, V](
      clientId: String = "consumer-client",
      groupId: String = "consumer-group"
  ): KafkaConsumer[IO, K, V] =
    new StubKafkaConsumer[K, V](consumerSettings[K, V](clientId, groupId))

  def streaming[K, V](
      records0: List[CommittableConsumerRecord[IO, K, V]],
      clientId: String = "consumer-client",
      groupId: String = "consumer-group"
  ): KafkaConsumer[IO, K, V] =
    new StubKafkaConsumer[K, V](consumerSettings[K, V](clientId, groupId)) {
      override def stream: Stream[IO, CommittableConsumerRecord[IO, K, V]] =
        Stream.emits(records0)

      override def partitionedStream: Stream[IO, Stream[IO, CommittableConsumerRecord[IO, K, V]]] =
        Stream.emit(Stream.emits(records0))

      override def partitionsMapStream
          : Stream[IO, Map[TopicPartition, Stream[IO, CommittableConsumerRecord[IO, K, V]]]] =
        Stream.emit(
          records0
            .groupBy(record =>
              new TopicPartition(record.record.topic, record.record.partition)
            )
            .view
            .mapValues(Stream.emits(_))
            .toMap
        )
    }

  def committableRecord[K, V](
      record: ConsumerRecord[K, V]
  ): CommittableConsumerRecord[IO, K, V] =
    CommittableConsumerRecord(
      record,
      CommittableOffset(
        new TopicPartition(record.topic, record.partition),
        new OffsetAndMetadata(record.offset + 1L),
        KafkaCommitter[IO](
          _ => IO.unit,
          IO.raiseError(new AssertionError("unexpected metadata"))
        )
      )
    )

  private def consumerSettings[K, V](
      clientId: String,
      groupId: String
  ): ConsumerSettings[IO, K, V] =
    ConsumerSettings[IO, K, V](
      GenericDeserializer.const[IO, K](null.asInstanceOf[K]),
      GenericDeserializer.const[IO, V](null.asInstanceOf[V])
    )
      .withClientId(clientId)
      .withGroupId(groupId)

}
