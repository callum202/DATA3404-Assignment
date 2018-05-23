#!/bin/bash
javac TopThreeAirports.java -classpath .:/usr/local/flink/lib/*
jar cfm output.jar MANIFEST.MF *.class
rm *.class