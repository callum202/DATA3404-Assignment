#!/bin/bash

#run ./scp build [Task Number] to run the build script.
#CHANGE LINE 8 TO REFLECT YOUR FOLDER SYSTEM
if [ $1 == "build" ]
then
  task=$2
  cd Task\ $task
  ./build.sh

#run ./scp scp [unikey] to transfer this script to the ssh
elif [ $1 == "scp" ]
then
  scp scp.sh $2@soit-hdp-pro-14.ucc.usyd.edu.au:/home/$2/

# run ./scp [Task Number] [Arg0] [Arg1] [Arg2] [Arg3](Optional) to run the jar on the cluster, optionally specifying a file name for the output file
# If [Task Number] is 3, and,
    # 3 args have been input: Task 3 will be run with args: [arg0](flightDataDir) [arg1](unikey)
      # and the output file name will default to MostPopularAircraftTypes([arg0]).txt
    # 4 args have been input: Task 3 will be run with 3 args: [arg0](flightDataDir) [arg1](unikey) [arg2](outputFileName)
      # and the output file name will be a custom name specified in [arg2]
# If [Task Number] is 1 or 2, and,
    # 4 args have been input, the task will be run with args: [arg0](year) [arg1](flightDataDir) [arg2](unikey)
      # and the output file name will default to [TaskName]([arg0]).txt
    # 5 args have been input, the task will be run with args: [arg0](year) [arg1](flightDataDir) [arg2](unikey) [arg3](outputFileName)
      # and the output file name will be a custom name specified in [arg3]
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


  if [ "$#" -eq 3 ]
  then
    $FLINK_HOME/bin/flink run -m yarn-cluster -yn 2 --class $class output$task.jar $arg0 $class"("$arg0")" $arg1
  elif [ "$#" -eq 4 ]
  then
    if [ $task -eq 3 ]
    then
      $FLINK_HOME/bin/flink run -m yarn-cluster -yn 2 --class $class output$task.jar $arg0 $arg2 $arg1
    else
      $FLINK_HOME/bin/flink run -m yarn-cluster -yn 2 --class $class output$task.jar $arg0 $arg1 $class"("$arg1")" $arg2
    fi
  elif [ "$#" -eq 5 ]
  then
    $FLINK_HOME/bin/flink run -m yarn-cluster -yn 2 --class $class output$task.jar $arg0 $arg1 $arg3 $arg2
  else
    exit
  fi
fi
