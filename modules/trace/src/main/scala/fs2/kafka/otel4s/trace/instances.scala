package fs2.kafka.otel4s.trace

import fs2.kafka.{Header, Headers}

import org.typelevel.otel4s.context.propagation.{TextMapGetter, TextMapUpdater}

/** Typeclass instances for using [[fs2.kafka.Headers]] as an otel4s propagation carrier.
  *
  * Trace context is propagated via Kafka headers, following the OpenTelemetry messaging specification. When multiple
  * Kafka headers share the same key, extraction prefers the last matching header so the most recently injected value
  * wins, matching OpenTelemetry Java Kafka instrumentation.
  *
  * @see
  *   [[https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/]]
  */
trait Otel4sInstances {

  implicit val headersTextMapGetter: TextMapGetter[Headers] =
    new TextMapGetter[Headers] {

      override def get(carrier: Headers, key: String): Option[String] =
        carrier.toChain.reverseIterator.find(_.key == key).flatMap(_.as[Option[String]])

      override def keys(carrier: Headers): Iterable[String] =
        carrier.toChain.map(_.key).toVector

    }

  implicit val headersTextMapUpdater: TextMapUpdater[Headers] =
    new TextMapUpdater[Headers] {

      override def updated(carrier: Headers, key: String, value: String): Headers =
        Headers.fromChain(carrier.toChain.filterNot(_.key == key) :+ Header(key, value))

    }

}

object instances extends Otel4sInstances
