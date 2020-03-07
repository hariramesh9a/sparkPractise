name := "spark_examples"

version := "0.1"

scalaVersion := "2.11.8"

resolvers += Resolver.bintrayRepo("clustering4ever", "C4E")
resolvers += "mvnrepository" at "http://mvnrepository.com/artifact/"

libraryDependencies ++= Seq(


  "org.apache.spark" %% "spark-core" % "2.4.4" % "provided",
  "org.apache.spark" %% "spark-mllib" % "2.4.4" % "runtime",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "org.apache.spark" %% "spark-mllib" % "2.4.4",
  "org.vegas-viz" %% "vegas" % "0.3.11",
  "org.vegas-viz" %% "vegas-spark" % "0.3.11",
  // https://mvnrepository.com/artifact/org.clustering4ever/clusteringscala
  "org.clustering4ever" %% "clusteringscala" % "0.9.6",
  "org.clustering4ever" % "clustering4ever_2.11" % "0.9.6"


)
