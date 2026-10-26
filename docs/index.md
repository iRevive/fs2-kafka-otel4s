# fs2-kafka-otel4s

OpenTelemetry tracing for [fs2-kafka](https://github.com/typelevel/fs2-kafka), using [otel4s](https://github.com/typelevel/otel4s).

## Quick start

Add the trace module dependency:

```scala
libraryDependencies += "io.github.irevive" %% "fs2-kafka-otel4s-trace" % "@VERSION@"
```

Supply a configured otel4s `TracerProvider[IO]`. The example uses Kafka at `localhost:9092` and an `orders` topic.

Import the tracing syntax and add `.traced(config)` before consuming records:

```scala mdoc:silent
import cats.effect.IO
import fs2.kafka.{ConsumerSettings, Deserializer, KafkaConsumer}
import fs2.kafka.KafkaConsumer._
import fs2.kafka.otel4s.trace.KafkaTracer
import fs2.kafka.otel4s.trace.syntax._
import org.typelevel.otel4s.trace.TracerProvider

val consumerSettings: ConsumerSettings[IO, String, String] =
  ConsumerSettings[IO, String, String](
    Deserializer[IO, String],
    Deserializer[IO, String]
  )
    .withBootstrapServers("localhost:9092")
    .withClientId("orders-consumer")
    .withGroupId("orders-group")

def consumeOrders(implicit tracerProvider: TracerProvider[IO]): IO[Nothing] =
  KafkaConsumer
    .stream[IO, String, String](consumerSettings)
    .subscribeTo("orders")
    .traced(KafkaTracer.Config.default)
    .consumeRecordTraced(record => IO.println(s"Order: ${record.value}"))
```

The handler runs once per record. Each chunk gets a receive span, one process span per handler, and a commit span.
Offsets are committed after all handlers succeed. A failed or canceled handler skips the commit.

For a send example, see [Producers](getting-started/producers.md).

## Next steps

- [Consumers](getting-started/consumers.md): handlers, commits, and span diagrams.
- [Interactive consumer guide](interactive-guides/consumer-tracing.md): configure tracing and inspect spans and links.
- [Producers](getting-started/producers.md): send records, choose a batch mode, and use transactions.
- [Interactive producer guide](interactive-guides/producer-tracing.md): compare batch spans and outgoing trace contexts.
- [Configuration](getting-started/configuration.md): control spans, attributes, message keys, and tracer reuse.
- [Low-level API](reference/low-level-api.md): trace operations in your own Kafka loop.
- [Producer instrumentation](reference/producer-instrumentation.md): inspect the detailed span and propagation rules.
