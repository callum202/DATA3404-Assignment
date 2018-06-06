#!/bin/bash
if [ "$#" -gt 0 ]
then
  user=$1
else
  user="hche8927"
fi
javac AverageFlightDelay.java -classpath .:/usr/local/flink/lib/*.:/Users/callumvandenhoek/Downloads/lib/lib/*:/Users/callumvandenhoek/hadoop-2.9.0/*
jar cfm output2.jar MANIFEST.MF *.class
rm *.class
scp output2.jar $user@soit-hdp-pro-14.ucc.usyd.edu.au:/home/$user/
