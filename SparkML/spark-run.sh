# !/bin/bash

# Script to package & submit a spark standalone application
# v1.0 17/08/16

echo
echo "Spark - building package & submiting to cluster"
echo "##############################################"
echo

# check for argumnet if supplied
if [ $# -eq 0 ]
then
	echo "no argument supplied. Please supply the spark project path"
	exit 1
fi

# set the project directory
PROJECT_DIR="$1"

# set the class name
CLASS_NAME="$2"

# set the jar name - will automate in future
JAR_NAME="$3"

# go into project path
cd "$PROJECT_DIR"

# running the sbt package
sbt package

# Using spark-submit for the clusters
spark-submit --class $CLASS_NAME --master local[4] $JAR_NAME


