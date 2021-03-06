name := "Project"

version := "0.1"

scalaVersion := "2.11.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.1" % "compile"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.7.2"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-annotations" % "2.7.2"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.7.2"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.7.2"

libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1"

libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1" classifier "models"
