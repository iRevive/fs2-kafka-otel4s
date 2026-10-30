import laika.helium.config.TextLink

ThisBuild / tlBaseVersion := "0.3"

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

ThisBuild / tlSitePublishBranch := Some("main")
ThisBuild / tlSitePublishTags := false

ThisBuild / githubWorkflowJavaVersions := Seq(JavaSpec.temurin("17"))

ThisBuild / githubWorkflowAddedJobs += WorkflowJob(
  id = "docs-configurator",
  name = "Test documentation configurator",
  steps = List(
    WorkflowStep.Checkout,
    WorkflowStep.Use(
      UseRef.Public("actions", "setup-node", "v6"),
      name = Some("Setup Node.js"),
      params = Map("node-version" -> "24")
    ),
    WorkflowStep.Run(
      List("node --test tests/site/*.test.cjs"),
      name = Some("Check configurator behavior")
    )
  ),
  scalas = Nil,
  javas = Nil,
)

lazy val Versions = new {
  val fs2kafka = "4.1.0"
  val otel4s = "1.1.0"

  val munit = "1.3.6"
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
  site,
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

lazy val site = project
  .in(file("modules/site"))
  .enablePlugins(TypelevelSitePlugin)
  .settings(
    mdocIn := file("docs"),
    mdocOut := target.value / "mdoc",
    laikaConfig := LaikaConfig.defaults.withRawContent,
    tlSiteHelium := tlSiteHelium.value.all
      .metadata(title = Some("fs2-kafka-otel4s"), language = Some("en"))
      .site
      .topNavigationBar(
        homeLink = TextLink.internal(laika.ast.Path.Root / "index.md", "fs2-kafka-otel4s")
      )
      .site
      .internalCSS(laika.ast.Path.Root / "site.css")
      .site
      .internalCSS(laika.ast.Path.Root / "assets" / "tracing-explorer.css")
      .site
      .internalJS(laika.ast.Path.Root / "assets" / "tracing-explorer.js")
      .site
      .internalJS(laika.ast.Path.Root / "assets" / "consumer-spans.js")
      .site
      .internalJS(laika.ast.Path.Root / "assets" / "producer-spans.js"),
  )
  .dependsOn(trace)
