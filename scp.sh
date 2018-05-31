#!/bin/bash

#run ./scp build [Task Number] to run the build script.
if [ $1 == "build" ]
then
  task=$2
  cd Task\ $task #CHANGE THIS LINE TO REFLECT YOUR FOLDER SYSTEM
  ./build.sh

#run ./scp [unikey] scp to transfer this script to the ssh
elif [ $1 == "scp" ]
then
  scp scp.sh $2@soit-hdp-pro-14.ucc.usyd.edu.au:/home/$2/

#run ./scp [Task Number] [Arg0] [Arg1] [Arg2] [Arg2] to run the jar on the cluster
else
  task=$1
  arg0=$2
  arg1=$3
  arg2=$4
  arg3=$5

  case $task in
    1) class="TopThreeAirports";;
    2) class="AverageFlightDelay";;
    3) class="MostPopularAircraftTypes";;
  esac


  $FLINK_HOME/bin/flink run -m yarn-cluster -yn 2 --class $class output$task.jar $arg0 $arg1 $arg2 $arg3
fi
