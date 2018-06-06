#!/bin/bash

#run ./scp build [Task Number] to run the build script.
#use argument 'bad' to build the 'bad' version of the code.
if [ $1 == "build" ]
then
  task=$2
  cd Task$task #CHANGE TO REFLECT YOUR FOLDER SYSTEM
  if [ "$#" -gt 3 ]
  then
    if [ $4 == "bad" ]
    then
      ./build.sh bad
    fi
  else
    ./build.sh $3
  fi

#run ./scp [unikey] scp to transfer this script to the ssh
elif [ $1 == "scp" ]
then
  scp scp.sh $2@soit-hdp-pro-14.ucc.usyd.edu.au:/home/$2/

# run ./scp [Task Number] [Arg0](Required) [Arg1](Optional) [Arg2](Optional) [Arg3](Optional) to run the jar on the cluster. See source code files for argument values.
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
