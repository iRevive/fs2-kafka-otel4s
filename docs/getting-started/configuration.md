# Configuration

Pass `KafkaTracer.Config.default` to `.traced(config)` to start. This creates one tracer per stream evaluation.
To reuse a tracer, pass it to `.traced(kafkaTracer)`.

```scala mdoc:silent
import fs2.kafka.otel4s.trace.{BatchSpanMode, KafkaTracer, ProcessSpanRelationship}

val tracerConfig: KafkaTracer.Config =
  KafkaTracer.Config
    .default
    .withServerAddress("localhost", Some(9092))

val lowerVolumeTracerConfig: KafkaTracer.Config =
  tracerConfig.withBatchSpanMode(BatchSpanMode.SharedSendSpan)

val processingOnlyConfig: KafkaTracer.Config =
  tracerConfig.withoutReceiveSpans.withoutCommitSpans
```

| Option | Use it to |
| --- | --- |
| `withServerAddress(host, port)` | Add explicit broker endpoint attributes; bootstrap servers are configured separately in Kafka settings |
| `withBatchSpanMode(SharedSendSpan)` | Reduce producer batch span volume; the default is `PerRecordSpans` |
| `withoutReceiveSpans`, `withoutProcessSpans`, `withoutCommitSpans` | Suppress consumer span types while retaining processing and commits |
| `withoutSendSpans` | Suppress producer send spans while retaining propagation and any batch create spans |
| `withConstAttributes`, `addConstAttributes` | Add attributes to library spans |
| `with*SpanSetup` | Customize a span's name, attributes, and finalization, or return `None` to suppress it |

`withProcessSpanSetup` controls record spans. `withBatchProcessSpanSetup` controls chunk spans.
`withoutProcessSpans` disables both.

Disabling spans still runs your effects. Use `KafkaTracer.noop` to disable both spans and header propagation.

Set client and group IDs in Kafka settings. Set tracing options in `KafkaTracer.Config`.

## Process span relationships

Set `SpanSetup.Process.relationship` through either process setup hook:

| Mode | Behavior |
| --- | --- |
| `Link` (default) | Start a root span and link to message context |
| `ParentFromRecord` | Use message context as parent, retaining the message link and linking any distinct valid ambient context |

```scala mdoc:silent
val perTopicConfig = tracerConfig.withProcessSpanSetup { ctx =>
  KafkaTracer.Config.Defaults.processSpanSetup(ctx).map { setup =>
    if (ctx.topic == "orders") setup.withRelationship(ProcessSpanRelationship.ParentFromRecord)
    else setup
  }
}
```

Use `withBatchProcessSpanSetup` independently for batch processing, with
`KafkaTracer.Config.Defaults.batchProcessSpanSetup(ctx)` as the starting point.
Parenting applies to single-record processing and singleton batches. Missing or invalid context produces a root;
multiple-record batches remain linked roots. Receive and commit behavior stays the same.

## Custom message keys

Define `KafkaMessageKey` to include custom keys in `messaging.kafka.message.key`. Return `None` to omit a key.

When changing key types, use `tracedWithSerializers` to keep key attributes. Plain `withSerializers` keeps spans
but stops deriving key attributes for the new type.

```scala mdoc:silent
import cats.effect.IO
import fs2.kafka.Serializer
import fs2.kafka.otel4s.trace.{KafkaMessageKey, TracedKafkaProducer}

final class OrderId(val value: String)

implicit val orderIdKafkaMessageKey: KafkaMessageKey[OrderId] =
  KafkaMessageKey.instance(id => Some(id.value))

def remapSerializers(
    producer: TracedKafkaProducer[IO, String, String]
): TracedKafkaProducer[IO, OrderId, String] =
  producer.tracedWithSerializers(
    Serializer[IO, String].contramap[OrderId](_.value),
    Serializer[IO, String]
  )
```

## Resource binding and tracer reuse

If you allocate clients through `Resource`, create a `KafkaTracer` once and bind your handles explicitly. The same
tracer can also be passed to `.traced(kafkaTracer)` in stream syntax.

```scala mdoc:silent
import cats.effect.Resource
import fs2.kafka.{ConsumerSettings, KafkaConsumer, KafkaProducer, ProducerSettings}
import fs2.kafka.otel4s.trace.TracedKafkaConsumer
import org.typelevel.otel4s.trace.TracerProvider

def createTracedProducer(
    producerSettings: ProducerSettings[IO, String, String]
)(implicit tracerProvider: TracerProvider[IO]): Resource[IO, TracedKafkaProducer[IO, String, String]] =
  for {
    kafkaTracer <- KafkaTracer.resource[IO](tracerConfig)
    producer <- KafkaProducer.resource[IO, String, String](producerSettings)
  } yield kafkaTracer.producer(producer)

def createTracedConsumer(
    consumerSettings: ConsumerSettings[IO, String, String]
)(implicit tracerProvider: TracerProvider[IO]): Resource[IO, TracedKafkaConsumer[IO, String, String]] =
  for {
    kafkaTracer <- KafkaTracer.resource[IO](tracerConfig)
    consumer <- KafkaConsumer.resource[IO, String, String](consumerSettings)
  } yield kafkaTracer.consumer(consumer)
```

Reuse traced handles for as long as their Kafka clients exist.

| Metadata provider | Lifetime |
| --- | --- |
| `Provider.fromConsumer` / `Provider.fromProducer` | Retains the client and caches its identity on first lookup, including failed lookups |
| `Provider.constant` | Stores supplied metadata without retaining a client |

For commits, use metadata from the consumer that produced the offsets. Use `ConsumerMetadata(None, None)`
when its identity is unavailable.

The commit batching method requires `Temporal` for timed flushes and `Parallel` for separate committers.
