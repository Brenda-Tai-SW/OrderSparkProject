name := "SparkTest"

version := "0.1"

scalaVersion := "2.12.14"

val sparkVersion = "3.1.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion
libraryDependencies += "net.liftweb" %% "lift-json" % "3.4.3"
libraryDependencies += "com.sun.mail" % "javax.mail" % "1.6.2"
libraryDependencies += "org.quartz-scheduler" % "quartz" % "2.3.2"
libraryDependencies += "com.microsoft.azure" % "azure-storage" % "8.6.5"
libraryDependencies +="org.apache.hadoop" % "hadoop-azure" % "3.2.0"
libraryDependencies +="org.apache.hadoop" % "hadoop-azure-datalake" % "3.2.0",