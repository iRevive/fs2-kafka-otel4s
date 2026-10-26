# fs2-kafka-otel4s

Trace fs2-kafka producers and consumers with otel4s. Producer adapters instrument publishing; consumer helpers make
processing boundaries explicit in your app.

## Explore consumer tracing

The [interactive consumer guide](consumer-tracing.md) shows which spans each helper creates, how long they remain
open, and which configuration controls them. Toggle spans and compare per-record processing with chunk processing.

## Install

```scala
libraryDependencies += "io.github.irevive" %% "fs2-kafka-otel4s-trace" % "@VERSION@"
```

Create a library tracer using your configured otel4s `TracerProvider`, then bind a consumer:

```scala mdoc:silent
import cats.effect.IO
import fs2.kafka.KafkaConsumer
import fs2.kafka.otel4s.trace.{KafkaTracer, TracedKafkaConsumer}
import org.typelevel.otel4s.trace.TracerProvider

def traceConsumer(
    consumer: KafkaConsumer[IO, String, String]
)(implicit provider: TracerProvider[IO]): IO[TracedKafkaConsumer[IO, String, String]] =
  KafkaTracer.create[IO](KafkaTracer.Config.default).map(_.consumer(consumer))
```

See the [README](https://github.com/iRevive/fs2-kafka-otel4s#readme) for producer usage, setup, and additional examples.

## Work on these docs

From the repository root:

```text
sbt 'site/tlSitePreview'
```

Open <http://localhost:4242>. The preview rebuilds as you edit Markdown, JavaScript, or CSS under `docs/site`.

To generate the static site for hosting:

```text
sbt 'site/tlSite'
```

The output is `modules/site/target/docs/site`. README generation remains `sbt 'docs/mdoc'`.
The site is built in CI; automatic publishing is disabled until hosting is configured.
