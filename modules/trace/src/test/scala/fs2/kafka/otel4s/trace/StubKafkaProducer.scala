package fs2.kafka.otel4s.trace

import cats.effect.{Deferred, IO, Ref, Resource}
import fs2.Chunk
import fs2.kafka._
import org.apache.kafka.clients.consumer.{ConsumerGroupMetadata, OffsetAndMetadata}
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.{Metric, MetricName, PartitionInfo, TopicPartition}

class StubKafkaProducer[K, V](
    override val settings: ProducerSettings[IO, K, V]
) extends KafkaProducer.WithSettings[IO, K, V] {

  override def produce(records: ProducerRecords[K, V]): IO[IO[ProducerResult[K, V]]] =
    unsupported

  override def initTransactions: IO[Unit] =
    unsupported

  override def transaction: Resource[IO, Unit] =
    Resource.eval(unsupported)

  override def sendOffsetsToTransaction(
      offsets: Map[TopicPartition, OffsetAndMetadata],
      consumerGroupMetadata: ConsumerGroupMetadata
  ): IO[Unit] =
    unsupported

  override def produceAndCommitTransactionally(
      records: TransactionalProducerRecords[IO, K, V]
  ): IO[ProducerResult[K, V]] =
    unsupported

  override def produceTransactionally(
      records: ProducerRecords[K, V]
  ): IO[ProducerResult[K, V]] =
    unsupported

  override def metrics: IO[Map[MetricName, Metric]] =
    unsupported

  override def partitionsFor(topic: String): IO[List[PartitionInfo]] =
    unsupported

  override def withSerializers[K2, V2](
      keySerializer: KeySerializer[IO, K2],
      valueSerializer: ValueSerializer[IO, V2]
  ): KafkaProducer.WithSettings[IO, K2, V2] =
    new StubKafkaProducer[K2, V2](
      settings.withSerializers(
        Resource.pure[IO, KeySerializer[IO, K2]](keySerializer),
        Resource.pure[IO, ValueSerializer[IO, V2]](valueSerializer)
      )
    )

  private def unsupported[A]: IO[A] =
    IO.raiseError(new AssertionError("StubKafkaProducer method should not be called"))

}

object StubKafkaProducer {

  trait Recorder[K, V] extends KafkaProducer.WithSettings[IO, K, V] {

    def getCaptured: IO[Chunk[ProducerRecord[K, V]]]
    def getCompletions: IO[Int]
    def getTransactionUses: IO[Int]
    def getSentOffsets: IO[Vector[(Map[TopicPartition, OffsetAndMetadata], ConsumerGroupMetadata)]]

  }

  trait GatedAwaitRecorder[K, V] extends Recorder[K, V] {

    def awaitStarted: IO[Unit]
    def awaitCanceled: IO[Unit]
    def releaseAwait: IO[Unit]

  }

  def metadataOnly[K, V](
      clientId: String = "producer-client"
  ): KafkaProducer.WithSettings[IO, K, V] =
    new StubKafkaProducer[K, V](producerSettings[K, V](clientId)) {
      override def produce(records: ProducerRecords[K, V]): IO[IO[ProducerResult[K, V]]] =
        IO.pure {
          IO.pure(
            records.zipWithIndex
              .map { case (record, index) =>
                val partition = record.partition.getOrElse(0)
                val metadata =
                  new RecordMetadata(
                    new TopicPartition(record.topic, partition),
                    index.toLong + 42L,
                    0,
                    0L,
                    0,
                    0
                  )

                record -> metadata
              }
          )
        }
    }

  def recorder[K, V](clientId: String = "producer-client"): IO[Recorder[K, V]] =
    for {
      captured <- Ref[IO].of(Chunk.empty[ProducerRecord[K, V]])
      completions <- Ref[IO].of(0)
      transactionUses <- Ref[IO].of(0)
      sentOffsets <- Ref[IO].of(
        Vector.empty[(Map[TopicPartition, OffsetAndMetadata], ConsumerGroupMetadata)]
      )
    } yield new RecorderImpl(producerSettings(clientId), captured, completions, transactionUses, sentOffsets)

  def failingAwait[K, V](
      cause: Throwable,
      clientId: String = "producer-client"
  ): KafkaProducer.WithSettings[IO, K, V] =
    new StubKafkaProducer[K, V](producerSettings[K, V](clientId)) {
      override def produce(records: ProducerRecords[K, V]): IO[IO[ProducerResult[K, V]]] =
        IO.pure(IO.raiseError(cause))
    }

  def failingOuter[K, V](
      cause: Throwable,
      clientId: String = "producer-client"
  ): KafkaProducer.WithSettings[IO, K, V] =
    new StubKafkaProducer[K, V](producerSettings[K, V](clientId)) {
      override def produce(records: ProducerRecords[K, V]): IO[IO[ProducerResult[K, V]]] =
        IO.raiseError(cause)
    }

  def gatedAwait[K, V](clientId: String = "producer-client"): IO[GatedAwaitRecorder[K, V]] =
    for {
      captured <- Ref[IO].of(Chunk.empty[ProducerRecord[K, V]])
      completions <- Ref[IO].of(0)
      transactionUses <- Ref[IO].of(0)
      sentOffsets <- Ref[IO].of(
        Vector.empty[(Map[TopicPartition, OffsetAndMetadata], ConsumerGroupMetadata)]
      )
      started <- Deferred[IO, Unit]
      released <- Deferred[IO, Unit]
      canceled <- Deferred[IO, Unit]
    } yield new GatedAwaitRecorderImpl(
      producerSettings(clientId),
      captured,
      completions,
      transactionUses,
      sentOffsets,
      started,
      released,
      canceled
    )

  private def producerSettings[K, V](clientId: String): ProducerSettings[IO, K, V] =
    ProducerSettings[IO, K, V](
      Serializer.asNull[IO, K],
      Serializer.asNull[IO, V]
    ).withClientId(clientId)

  private final class RecorderImpl[K, V](
      settings: ProducerSettings[IO, K, V],
      captured: Ref[IO, Chunk[ProducerRecord[K, V]]],
      completions: Ref[IO, Int],
      transactionUses: Ref[IO, Int],
      sentOffsets: Ref[IO, Vector[(Map[TopicPartition, OffsetAndMetadata], ConsumerGroupMetadata)]]
  ) extends StubKafkaProducer[K, V](settings)
      with Recorder[K, V] {

    override def getCaptured: IO[Chunk[ProducerRecord[K, V]]] =
      captured.get

    override def getCompletions: IO[Int] =
      completions.get

    override def getTransactionUses: IO[Int] =
      transactionUses.get

    override def getSentOffsets: IO[Vector[(Map[TopicPartition, OffsetAndMetadata], ConsumerGroupMetadata)]] =
      sentOffsets.get

    override def withSerializers[K2, V2](
        keySerializer: KeySerializer[IO, K2],
        valueSerializer: ValueSerializer[IO, V2]
    ): KafkaProducer.WithSettings[IO, K2, V2] = {
      val capt = Ref.lens[IO, Chunk[ProducerRecord[K, V]], Chunk[ProducerRecord[K2, V2]]](captured)(
        _.asInstanceOf[Chunk[ProducerRecord[K2, V2]]],
        _ => _.asInstanceOf[Chunk[ProducerRecord[K, V]]]
      )
      new RecorderImpl(
        settings.withSerializers(
          Resource.pure[IO, KeySerializer[IO, K2]](keySerializer),
          Resource.pure[IO, ValueSerializer[IO, V2]](valueSerializer)
        ),
        capt,
        completions,
        transactionUses,
        sentOffsets
      )
    }

    override def produce(records: ProducerRecords[K, V]): IO[IO[ProducerResult[K, V]]] =
      captured.set(records).as(completions.update(_ + 1).as(Chunk.empty))

    override def transaction: Resource[IO, Unit] =
      Resource.make(transactionUses.update(_ + 1))(_ => IO.unit)

    override def sendOffsetsToTransaction(
        offsets: Map[TopicPartition, OffsetAndMetadata],
        consumerGroupMetadata: ConsumerGroupMetadata
    ): IO[Unit] =
      sentOffsets.update(_ :+ (offsets -> consumerGroupMetadata))

  }

  private final class GatedAwaitRecorderImpl[K, V](
      settings: ProducerSettings[IO, K, V],
      captured: Ref[IO, Chunk[ProducerRecord[K, V]]],
      completions: Ref[IO, Int],
      transactionUses: Ref[IO, Int],
      sentOffsets: Ref[IO, Vector[(Map[TopicPartition, OffsetAndMetadata], ConsumerGroupMetadata)]],
      started: Deferred[IO, Unit],
      released: Deferred[IO, Unit],
      canceled: Deferred[IO, Unit]
  ) extends StubKafkaProducer[K, V](settings)
      with GatedAwaitRecorder[K, V] {

    override def getCaptured: IO[Chunk[ProducerRecord[K, V]]] =
      captured.get

    override def getCompletions: IO[Int] =
      completions.get

    override def getTransactionUses: IO[Int] =
      transactionUses.get

    override def getSentOffsets: IO[Vector[(Map[TopicPartition, OffsetAndMetadata], ConsumerGroupMetadata)]] =
      sentOffsets.get

    override def awaitStarted: IO[Unit] =
      started.get

    override def awaitCanceled: IO[Unit] =
      canceled.get

    override def releaseAwait: IO[Unit] =
      released.complete(()).void

    override def produce(records: ProducerRecords[K, V]): IO[IO[ProducerResult[K, V]]] =
      captured.set(records).as {
        started.complete(()).void *>
          released.get.onCancel(canceled.complete(()).void) *>
          completions.update(_ + 1).as(Chunk.empty)
      }

  }
}
