ThisBuild / tlBaseVersion := "0.1"

ThisBuild / organization := "io.github.irevive"
ThisBuild / licenses := Seq(License.Apache2)
ThisBuild / developers := List(
  // your GitHub handle and name
  tlGitHubDev("iRevive", "Maksym Ochenashko")
)
ThisBuild / startYear := Some(2026)

val Scala213 = "2.13.18"
ThisBuild / crossScalaVersions := Seq(Scala213, "3.3.7")
ThisBuild / scalaVersion := Scala213 // the default Scala

ThisBuild / tlCiDependencyGraphJob := false

ThisBuild / resolvers += Resolver.sonatypeCentralSnapshots

lazy val Versions = new {
  val fs2kafka = "4.1-078c324-SNAPSHOT"
  val otel4s = "1.0.0"

  val munit = "1.2.4"
  val munitCatsEffect = "2.2.0"
}

lazy val munitDependencies = Def.settings(
  libraryDependencies ++= Seq(
    "org.scalameta" %% "munit" % Versions.munit % Test,
    "org.typelevel" %% "munit-cats-effect" % Versions.munitCatsEffect % Test,
  )
)

lazy val root = tlCrossRootProject.aggregate(
  trace,
  docs,
)

lazy val trace = project
  .enablePlugins(BuildInfoPlugin)
  .in(file("modules/trace"))
  .settings(munitDependencies)
  .settings(
    name := "fs2-kafka-otel4s-trace",
    libraryDependencies ++= Seq(
      "org.typelevel" %% "fs2-kafka" % Versions.fs2kafka,
      "org.typelevel" %% "otel4s-core-trace" % Versions.otel4s,
      "org.typelevel" %% "otel4s-semconv" % Versions.otel4s,
      "org.typelevel" %% "otel4s-oteljava-testkit" % Versions.otel4s % Test,
      "org.typelevel" %% "otel4s-semconv-experimental" % Versions.otel4s % Test,
    ),
    buildInfoPackage := "fs2.kafka.otel4s.trace",
    buildInfoOptions += sbtbuildinfo.BuildInfoOption.PackagePrivate,
    buildInfoKeys := Seq[BuildInfoKey](
      "version" -> version
    )
  )

lazy val docs = project
  .in(file("modules/docs"))
  .enablePlugins(MdocPlugin, NoPublishPlugin)
  .settings(
    mdocIn := file("docs/index.md"),
    mdocOut := file("README.md"),
    mdocVariables := Map(
      "VERSION" -> version.value
    ),
  )
  .dependsOn(trace)

/*
lazy val docs = project
  .in(file("site"))
  .enablePlugins(TypelevelSitePlugin)
  .dependsOn(
    oteljava,
    `oteljava-context-storage`,
    `oteljava-context-storage-testkit`,
    `oteljava-testkit`,
    `instrumentation-metrics`.jvm,
    `semconv-stable`.jvm,
    `semconv-metrics-stable`.jvm,
  )
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-http" % PekkoHttpVersion,
      "org.http4s" %% "http4s-client" % Http4sVersion,
      "org.http4s" %% "http4s-dsl" % Http4sVersion,
      "io.opentelemetry" % "opentelemetry-sdk-extension-autoconfigure" % OpenTelemetryVersion,
      "io.opentelemetry.instrumentation" % "opentelemetry-instrumentation-annotations" % OpenTelemetryInstrumentationVersion,
      "io.opentelemetry.instrumentation" % "opentelemetry-runtime-telemetry-java8" % OpenTelemetryInstrumentationAlphaVersion,
      "io.opentelemetry.instrumentation" % "opentelemetry-runtime-telemetry-java17" % OpenTelemetryInstrumentationAlphaVersion,
      "com.outr" %% "scribe" % ScribeVersion,
      // a trick to make Scala-Steward provide updates for this dependency
      "io.github.irevive" % "otel4s-opentelemetry-javaagent" % Otel4sAgentVersion % Test
    ),
    mdocVariables ++= Map(
      "OPEN_TELEMETRY_VERSION" -> OpenTelemetryVersion,
      "OPEN_TELEMETRY_INSTRUMENTATION_ALPHA_VERSION" -> OpenTelemetryInstrumentationAlphaVersion,
      "OTEL4S_AGENT_VERSION" -> Otel4sAgentVersion,
    ),
    tlSiteApiPackage := Some("org.typelevel.otel4s"),
    run / fork := true,
    javaOptions += "-Dcats.effect.trackFiberContext=true",
    laikaConfig := {
      import laika.config.{ChoiceConfig, Selections, SelectionConfig}

      laikaConfig.value.withConfigValue(
        Selections(
          SelectionConfig(
            "build-tool",
            ChoiceConfig("sbt", "sbt"),
            ChoiceConfig("scala-cli", "Scala CLI")
          ).withSeparateEbooks,
          SelectionConfig(
            "sdk-options-source",
            ChoiceConfig("sbt", "sbt"),
            ChoiceConfig("scala-cli", "Scala CLI"),
            ChoiceConfig("shell", "Shell")
          ).withSeparateEbooks,
          SelectionConfig(
            "scala-version",
            ChoiceConfig("scala-2", "Scala 2"),
            ChoiceConfig("scala-3", "Scala 3")
          ).withSeparateEbooks,
        )
      )
    }
  )

 */
