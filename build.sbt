name := "perS3"

version := "1.0"

scalaVersion := "2.10.3"

resolvers += "Maven Releases" at "http://repo.typesafe.com/typesafe/maven-releases"

libraryDependencies ++= Seq(
  "com.amazonaws"   %   "aws-java-sdk"    % "1.5.1"
  , "org.scalaz"    %%  "scalaz-core"     % "7.1.0"
  , "org.slf4j"     %   "slf4j-log4j12"   % "1.7.7"
)

