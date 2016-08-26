javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven/"

lazy val root = (project in file(".")).
  settings(
    name := "example-spark-error",
    version := "1.0",
    scalaVersion := "2.10.6",
    retrieveManaged := true,
    ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) },
    libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1"
  )
