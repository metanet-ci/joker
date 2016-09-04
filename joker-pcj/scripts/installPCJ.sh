#!/bin/bash

rm -rf pcj-jar-tmp/
mkdir pcj-jar-tmp/
cd pcj-jar-tmp/
git clone https://github.com/metanet/PCJ.git
cd PCJ
git checkout v4
./gradlew assemble
mvn install:install-file -DgroupId=icm.edu.pl -DartifactId=pcj -Dversion=4.1.0-METANET -Dpackaging=jar -Dfile=build/libs/PCJ-4.1.0.SNAPSHOT-bin.jar
cd ../..
rm -rf pcj-jar-tmp/
