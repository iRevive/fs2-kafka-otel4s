# fs2-kafka-otel4s

[![fs2-kafka-otel4s-trace Scala version support](https://index.scala-lang.org/irevive/fs2-kafka-otel4s/fs2-kafka-otel4s-trace/latest.svg)](https://index.scala-lang.org/irevive/fs2-kafka-otel4s/fs2-kafka-otel4s-trace)

OpenTelemetry tracing for [fs2-kafka](https://github.com/typelevel/fs2-kafka), using [otel4s](https://github.com/typelevel/otel4s).

## Quick start

Add the tracing module, replacing `<version>` with the version you use:

```scala
libraryDependencies += "io.github.irevive" %% "fs2-kafka-otel4s-trace" % "<version>"
```

Use your normal fs2-kafka consumer settings, including deserializers, bootstrap servers, and a consumer group ID.
With Kafka running and an `orders` topic available:

```scala
import cats.effect.IO
import fs2.kafka.{ConsumerSettings, KafkaConsumer}
import fs2.kafka.KafkaConsumer._
import fs2.kafka.otel4s.trace.KafkaTracer
import fs2.kafka.otel4s.trace.syntax._
import org.typelevel.otel4s.trace.TracerProvider

def consumeOrders(
    settings: ConsumerSettings[IO, String, String]
)(implicit provider: TracerProvider[IO]): IO[Nothing] =
  KafkaConsumer
    .stream[IO, String, String](settings)
    .subscribeTo("orders")
    .traced(KafkaTracer.Config.default)
    .consumeRecordTraced(record => IO.println(record.value))
```

Supply an otel4s `TracerProvider[IO]` with your backend and exporter configured.

`consumeRecordTraced` commits each consumed chunk after all its record handlers succeed. A failed or canceled
handler skips that commit. The consumer runs until it fails or is canceled.

Consumer receive and process spans start independent traces and link to recognized incoming message contexts.
Your tracing backend must support span links to display these connections.

This README describes the current branch. Check the documentation for the version you depend on when using a release.

## Documentation

See the [documentation site](https://irevive.github.io/fs2-kafka-otel4s/) for setup, examples, and API details.
Use the interactive guides to inspect [consumer spans](https://irevive.github.io/fs2-kafka-otel4s/interactive-guides/consumer-tracing.html)
and [producer spans](https://irevive.github.io/fs2-kafka-otel4s/interactive-guides/producer-tracing.html).
