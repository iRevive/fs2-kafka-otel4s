ThisBuild / tlBaseVersion := "0.2"

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

ThisBuild / githubWorkflowJavaVersions := Seq(JavaSpec.temurin("17"))

lazy val Versions = new {
  val fs2kafka = "4.1.0-RC1"
  val otel4s = "1.0.1"

  val munit = "1.3.3"
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
      "version" -> version.value
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
