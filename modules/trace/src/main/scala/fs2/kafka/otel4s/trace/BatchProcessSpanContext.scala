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

/** Record and consumer metadata used to configure one `process` span for a chunk callback.
  *
  * Passed to [[KafkaTracer.Config.withBatchProcessSpanSetup]] by [[ConsumerSpans.processBatch]], including when the
  * chunk contains a single record. Empty chunks do not invoke the setup function. Record-specific fields are available
  * only for a singleton chunk; no representative record is selected for a larger batch.
  */
sealed trait BatchProcessSpanContext {

  /** Distinct topic names present in the chunk. */
  def topics: Set[String]

  /** Distinct partition numbers present in the chunk.
    *
    * Partition numbers are local to a topic. A singleton set does not imply that all records belong to the same
    * topic-partition when [[topics]] contains several topics.
    */
  def partitions: Set[Int]

  /** Number of records in the nonempty chunk, including `1` for a singleton batch. */
  def recordCount: Int

  /** Canonical message key for a singleton chunk, when [[KafkaMessageKey]] can represent it; otherwise `None`. */
  def messageKey: Option[String]

  /** Record offset for a singleton chunk; `None` for a batch containing several records. */
  def offset: Option[Long]

  /** Effective Kafka `client.id` supplied by the metadata provider, when available. */
  def clientId: Option[String]

  /** Kafka `group.id` supplied by the metadata provider, when available. */
  def groupId: Option[String]
}

object BatchProcessSpanContext {
  private[otel4s] def apply(
      topics: Set[String],
      partitions: Set[Int],
      recordCount: Int,
      messageKey: Option[String],
      offset: Option[Long],
      clientId: Option[String],
      groupId: Option[String]
  ): BatchProcessSpanContext = Impl(topics, partitions, recordCount, messageKey, offset, clientId, groupId)

  private final case class Impl(
      topics: Set[String],
      partitions: Set[Int],
      recordCount: Int,
      messageKey: Option[String],
      offset: Option[Long],
      clientId: Option[String],
      groupId: Option[String]
  ) extends BatchProcessSpanContext
}
