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

/** Metadata passed to the commit span setup function.
  *
  * Commits made by the consume helpers include information about the processed records. `commitOffsets` only has
  * topic-partition entries, so record count, message key, and record offset are unknown.
  */
sealed trait CommitSpanContext {

  /** Topics being committed.
    */
  def topics: Set[String]

  /** Partition numbers being committed. The same number can appear in several topics.
    */
  def partitions: Set[Int]

  /** Number of records represented by this commit, when known.
    *
    * Offset-only commits cannot determine a record count from offsets. `None` means unknown; it does not mean zero.
    */
  def recordCount: Option[Int]

  /** Message key as a string when committing one known record and its key can be converted safely. Always `None` for
    * `commitOffsets`.
    */
  def messageKey: Option[String]

  /** Offset of the record when committing one known record. Always `None` for `commitOffsets`. This is a record offset,
    * not the next position sent to Kafka when committing.
    */
  def offset: Option[Long]

  /** Kafka `client.id` from the metadata provider, when available.
    */
  def clientId: Option[String]

  /** Kafka `group.id` from the metadata provider, when available.
    */
  def groupId: Option[String]

}

object CommitSpanContext {

  private[otel4s] def apply(
      topics: Set[String],
      partitions: Set[Int],
      recordCount: Option[Int],
      messageKey: Option[String],
      offset: Option[Long],
      clientId: Option[String],
      groupId: Option[String]
  ): CommitSpanContext =
    Impl(
      topics,
      partitions,
      recordCount,
      messageKey,
      offset,
      clientId,
      groupId
    )

  final private case class Impl(
      topics: Set[String],
      partitions: Set[Int],
      recordCount: Option[Int],
      messageKey: Option[String],
      offset: Option[Long],
      clientId: Option[String],
      groupId: Option[String]
  ) extends CommitSpanContext

}
