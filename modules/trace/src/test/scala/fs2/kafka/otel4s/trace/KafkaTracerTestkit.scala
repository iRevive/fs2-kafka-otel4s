package fs2.kafka.otel4s.trace

import cats.effect.{IO, Resource}
import fs2.kafka.KafkaProducer
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.propagation.TextMapPropagator
import io.opentelemetry.sdk.trace.data.SpanData
import org.typelevel.otel4s.oteljava.testkit.OtelJavaTestkit
import org.typelevel.otel4s.trace.{Tracer, TracerProvider}

final class KafkaTracerTestkit(testkit: OtelJavaTestkit[IO])(implicit
    val tracerProvider: TracerProvider[IO],
    val appTracer: Tracer[IO]
) {

  def tracedProducer[K: KafkaMessageKey, V](
      producer: KafkaProducer.WithSettings[IO, K, V],
      config: KafkaTracer.Config = KafkaTracer.Config.default
  ): IO[TracedKafkaProducer[IO, K, V]] =
    KafkaTracer.create[IO](config).map(_.producer(producer))

  def finishedSpans: IO[List[SpanData]] =
    testkit.finishedSpans

}

object KafkaTracerTestkit {

  def create(
      appTracerName: String = "fs2.kafka.otel4s.tests",
      propagators: Seq[TextMapPropagator] = Seq(W3CTraceContextPropagator.getInstance())
  ): Resource[IO, KafkaTracerTestkit] =
    OtelJavaTestkit
      .inMemory[IO](
        _.addTextMapPropagators(propagators*)
      )
      .evalMap { testkit =>
        for {
          tracer <- testkit.tracerProvider.get(appTracerName)
        } yield new KafkaTracerTestkit(testkit)(testkit.tracerProvider, tracer)
      }

}
