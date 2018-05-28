#!/bin/bash
javac MostPopularAircraftTypes.java -classpath .:/usr/local/flink/lib/*
jar cfm output.jar MANIFEST.MF *.class
rm *.class