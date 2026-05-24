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

/** Consumer-side metadata used to derive a tracing `process` span.
  *
  * This context describes a single consumed record at the application processing boundary. It carries the record
  * coordinates used by Kafka semantic conventions together with static consumer metadata captured from the bound Kafka
  * consumer.
  */
sealed trait ProcessSpanContext {

  /** Topic of the processed record.
    */
  def topic: String

  /** Partition of the processed record.
    */
  def partition: Int

  /** Offset of the processed record.
    */
  def offset: Long

  /** Canonical string form of the Kafka message key when it can be represented safely.
    */
  def messageKey: Option[String]

  /** Kafka `client.id` captured from the bound consumer, when configured.
    */
  def clientId: Option[String]

  /** Kafka `group.id` captured from the bound consumer, when configured.
    */
  def groupId: Option[String]

}

object ProcessSpanContext {

  private[otel4s] def apply(
      topic: String,
      partition: Int,
      offset: Long,
      messageKey: Option[String],
      clientId: Option[String],
      groupId: Option[String]
  ): ProcessSpanContext =
    Impl(topic, partition, offset, messageKey, clientId, groupId)

  final private case class Impl(
      topic: String,
      partition: Int,
      offset: Long,
      messageKey: Option[String],
      clientId: Option[String],
      groupId: Option[String]
  ) extends ProcessSpanContext

}
