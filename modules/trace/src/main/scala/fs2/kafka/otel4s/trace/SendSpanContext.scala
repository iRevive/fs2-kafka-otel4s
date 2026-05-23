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

/** Producer-side metadata used to derive a tracing `send` span.
  *
  * This context describes the produced batch at the point where `fs2-kafka-otel4s` is about to create a producer span.
  * It combines dynamic record information such as topics, partitions, and batch size with static producer metadata
  * captured from the bound Kafka producer.
  */
sealed trait SendSpanContext {

  /** Topics covered by the produced records.
    */
  def topics: Set[String]

  /** Partitions covered by the produced records, when explicitly set on the records.
    */
  def partitions: Set[Int]

  /** Number of records in the produced batch.
    */
  def recordCount: Int

  /** Canonical string form of the Kafka message key when this span describes a single record and the key can be
    * represented safely.
    */
  def messageKey: Option[String]

  /** Kafka `client.id` captured from the bound producer, when configured.
    */
  def clientId: Option[String]

}

object SendSpanContext {

  private[otel4s] def apply(
      topics: Set[String],
      partitions: Set[Int],
      recordCount: Int,
      messageKey: Option[String],
      clientId: Option[String]
  ): SendSpanContext =
    Impl(topics, partitions, recordCount, messageKey, clientId)

  final private case class Impl(
      topics: Set[String],
      partitions: Set[Int],
      recordCount: Int,
      messageKey: Option[String],
      clientId: Option[String]
  ) extends SendSpanContext

}
