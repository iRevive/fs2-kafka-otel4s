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

/** Controls how process spans relate to trace context propagated in message headers.
  *
  * Applies to single-record processing and singleton process batches. Batches containing multiple records always create
  * root spans linked to recognized message contexts. Receive and commit spans are unaffected.
  */
sealed abstract class ProcessSpanRelationship

object ProcessSpanRelationship {

  /** Creates a root process span linked to the record context. This is the default. */
  case object Link extends ProcessSpanRelationship

  /** Uses the record context as parent while retaining the message link.
    *
    * A distinct valid ambient context is also linked. Missing or invalid record context produces a root span.
    */
  case object ParentFromRecord extends ProcessSpanRelationship

}
