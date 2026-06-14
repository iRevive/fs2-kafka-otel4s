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

import cats.Parallel
import cats.effect.{Concurrent, Resource}
import cats.syntax.functor._
import cats.syntax.semigroup._
import fs2.kafka.{KafkaConsumer, KafkaProducer}
import org.typelevel.otel4s.semconv.attributes.{ErrorAttributes, ServerAttributes}
import org.typelevel.otel4s.trace.{SpanFinalizer, StatusCode, Tracer, TracerProvider}
import org.typelevel.otel4s.{Attribute, Attributes}

/** A [[KafkaTracer]] is created from an otel4s [[org.typelevel.otel4s.trace.TracerProvider]] using an instrumentation
  * scope managed entirely by this library. The tracing behavior can be customized with [[KafkaTracer.Config]] while
  * keeping the instrumentation identity stable.
  *
  * See the OpenTelemetry messaging and Kafka semantic conventions:
  *
  * @see
  *   [[https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/]]
  *
  * @see
  *   [[https://opentelemetry.io/docs/specs/semconv/messaging/kafka/]]
  */
trait KafkaTracer[F[_]] {

  /** Creates a producer-bound tracing handle.
    *
    * The handle captures static producer metadata such as `client.id` from the producer itself and traces the
    * producer's standard two-stage `produce` operations.
    */
  def producer[K: KafkaMessageKey, V](
      producer: KafkaProducer.WithSettings[F, K, V]
  ): TracedKafkaProducer[F, K, V]

  /** Creates a consumer-bound tracing handle.
    *
    * The handle captures static consumer metadata such as `client.id` and `group.id` from the consumer itself while
    * keeping `receive` and `process` tracing explicit.
    */
  def consumer[K: KafkaMessageKey, V](
      consumer: KafkaConsumer[F, K, V]
  ): TracedKafkaConsumer[F, K, V]

}

object KafkaTracer {

  /** Configuration for [[KafkaTracer]].
    *
    * Constant attributes configured here are attached to every span emitted by the library. Kafka client metadata
    * derived from `KafkaProducer` or `KafkaConsumer` is intentionally not configured here; it is captured by the traced
    * producer or consumer bound from that client.
    */
  sealed trait Config {

    private[otel4s] def tracerName: String
    private[otel4s] def constAttributes: Attributes
    private[otel4s] def sendSpanSetup: SendSpanContext => Config.SpanSetup
    private[otel4s] def receiveSpanSetup: ReceiveSpanContext => Config.SpanSetup
    private[otel4s] def processSpanSetup: ProcessSpanContext => Config.SpanSetup

    /** Replaces the constant attributes attached to every span emitted by this library.
      *
      * When these attributes use the same keys as metadata derived from the bound producer or consumer, the configured
      * values take precedence.
      */
    def withConstAttributes(attributes: Attributes): Config

    /** Appends constant attributes to every span emitted by this library.
      *
      * When these attributes use the same keys as metadata derived from the bound producer or consumer, the configured
      * values take precedence.
      */
    def addConstAttributes(head: Attribute[?], tail: Attribute[?]*): Config

    /** Replaces the function used to derive producer-side `send` span setup from record metadata.
      */
    def withSendSpanSetup(f: SendSpanContext => Config.SpanSetup): Config

    /** Replaces the function used to derive consumer-side `poll` / `receive` span setup from chunk metadata.
      */
    def withReceiveSpanSetup(f: ReceiveSpanContext => Config.SpanSetup): Config

    /** Replaces the function used to derive consumer-side `process` span setup from record metadata.
      */
    def withProcessSpanSetup(f: ProcessSpanContext => Config.SpanSetup): Config

    /** Adds `server.address` and, when provided, `server.port` to emitted spans.
      *
      * Use values derived from the logical Kafka broker or service address, not connection-level peer information.
      *
      * @example
      *   {{{
      * val withPort = KafkaTracer.Config.default.withServerAddress("kafka.internal", Some(9092))
      * val socket = KafkaTracer.Config.default.withServerAddress("/run/kafka.sock", None)
      *   }}}
      */
    def withServerAddress(serverAddress: String, serverPort: Option[Int]): Config

  }

  object Config {

    object Defaults {

      val tracerName: String = "fs2.kafka"

      val sendSpanSetup: SendSpanContext => SpanSetup =
        ctx => SpanSetup("send", Option.when(ctx.topics.size == 1)(ctx.topics.head))

      val receiveSpanSetup: ReceiveSpanContext => SpanSetup =
        ctx => SpanSetup("poll", Option.when(ctx.topics.size == 1)(ctx.topics.head))

      val processSpanSetup: ProcessSpanContext => SpanSetup =
        ctx => SpanSetup("process", Some(ctx.topic))

      val spanFinalizationStrategy: SpanFinalizer.Strategy = {
        case Resource.ExitCase.Errored(e) =>
          val errorType = Option(e.getClass.getCanonicalName).getOrElse(e.getClass.getName)

          val setStatus = Option(e.getMessage)
            .map(message => SpanFinalizer.setStatus(StatusCode.Error, message))
            .getOrElse(SpanFinalizer.setStatus(StatusCode.Error))

          SpanFinalizer.recordException(e) |+|
            SpanFinalizer.addAttribute(ErrorAttributes.ErrorType(errorType)) |+|
            setStatus

        case Resource.ExitCase.Canceled =>
          SpanFinalizer.addAttribute(ErrorAttributes.ErrorType("canceled")) |+|
            SpanFinalizer.setStatus(StatusCode.Error, "canceled")
      }

    }

    sealed trait SpanSetup {

      /** Final span name passed to the otel4s span builder.
        */
      def spanName: String

      /** Extra attributes attached in addition to fs2-kafka's semantic-convention attributes and configured constant
        * attributes.
        *
        * When duplicate keys exist, these attributes take precedence over both.
        */
      def attributes: Attributes

      /** Finalization strategy applied when building the span.
        */
      def finalizationStrategy: SpanFinalizer.Strategy

    }

    object SpanSetup {

      def apply(
          spanName: String,
          attributes: Attributes,
          finalizationStrategy: SpanFinalizer.Strategy
      ): SpanSetup =
        SpanSetupImpl(spanName, attributes, finalizationStrategy)

      private[KafkaTracer] def apply(operation: String, topic: Option[String]): SpanSetup =
        SpanSetup(
          spanName = topic.fold(operation)(value => s"$operation $value"),
          attributes = Attributes.empty,
          finalizationStrategy = Defaults.spanFinalizationStrategy
        )

      final private case class SpanSetupImpl(
          spanName: String,
          attributes: Attributes,
          finalizationStrategy: SpanFinalizer.Strategy
      ) extends SpanSetup

    }

    val default: Config =
      ConfigImpl(
        tracerName = Defaults.tracerName,
        constAttributes = Attributes.empty,
        sendSpanSetup = Defaults.sendSpanSetup,
        receiveSpanSetup = Defaults.receiveSpanSetup,
        processSpanSetup = Defaults.processSpanSetup,
      )

    final private case class ConfigImpl(
        tracerName: String,
        constAttributes: Attributes,
        sendSpanSetup: SendSpanContext => Config.SpanSetup,
        receiveSpanSetup: ReceiveSpanContext => Config.SpanSetup,
        processSpanSetup: ProcessSpanContext => Config.SpanSetup,
    ) extends Config {

      override def withConstAttributes(attributes: Attributes): Config =
        copy(constAttributes = attributes)

      override def addConstAttributes(head: Attribute[?], tail: Attribute[?]*): Config =
        copy(constAttributes = constAttributes + head ++ tail)

      override def withSendSpanSetup(f: SendSpanContext => Config.SpanSetup): Config =
        copy(sendSpanSetup = f)

      override def withReceiveSpanSetup(f: ReceiveSpanContext => Config.SpanSetup): Config =
        copy(receiveSpanSetup = f)

      override def withProcessSpanSetup(f: ProcessSpanContext => Config.SpanSetup): Config =
        copy(processSpanSetup = f)

      override def withServerAddress(serverAddress: String, serverPort: Option[Int]): Config =
        copy(
          constAttributes = constAttributes +
            ServerAttributes.ServerAddress(serverAddress) ++
            ServerAttributes.ServerPort.maybe(serverPort.map(_.toLong))
        )

    }

  }

  def apply[F[_]](implicit ev: KafkaTracer[F]): KafkaTracer[F] = ev

  /** Creates a library-managed [[KafkaTracer]] from the implicit otel4s [[org.typelevel.otel4s.trace.TracerProvider]].
    *
    * The returned tracer is not yet bound to a specific Kafka producer. Bind it to a [[fs2.kafka.KafkaProducer]] so the
    * resulting spans can include static client metadata such as `client.id`. If you want to attach logical broker
    * endpoint attributes such as `server.address` and `server.port`, provide them explicitly through
    * [[KafkaTracer.Config.withServerAddress]].
    */
  def create[F[_]: Concurrent: Parallel: TracerProvider](
      config: Config
  ): F[KafkaTracer[F]] =
    TracerProvider[F]
      .tracer(config.tracerName)
      .withVersion(BuildInfo.version)
      .get
      .map { implicit tracer =>
        new Impl[F](config)
      }

  /** Creates a library-managed [[KafkaTracer]] from the implicit otel4s [[org.typelevel.otel4s.trace.TracerProvider]].
    *
    * The returned tracer is not yet bound to a specific Kafka producer. Bind it to a [[fs2.kafka.KafkaProducer]] so the
    * resulting spans can include static client metadata such as `client.id`. If you want to attach logical broker
    * endpoint attributes such as `server.address` and `server.port`, provide them explicitly through
    * [[KafkaTracer.Config.withServerAddress]].
    */
  def resource[F[_]: Concurrent: Parallel: TracerProvider](
      config: Config
  ): Resource[F, KafkaTracer[F]] =
    Resource.eval(create(config))

  final private class Impl[F[_]: Concurrent: Parallel: Tracer](config: Config) extends KafkaTracer[F] {

    override def producer[K: KafkaMessageKey, V](
        producer: KafkaProducer.WithSettings[F, K, V]
    ): TracedKafkaProducer[F, K, V] =
      new TracedKafkaProducer.Impl[F, K, V](
        producer,
        config
      )

    override def consumer[K: KafkaMessageKey, V](
        consumer: KafkaConsumer[F, K, V]
    ): TracedKafkaConsumer[F, K, V] =
      new TracedKafkaConsumer.Impl[F, K, V](
        consumer,
        config
      )

  }

}
