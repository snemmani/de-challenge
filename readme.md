[![Build Status](https://travis-ci.org/snemmani/de-challenge.svg?branch=master)](https://travis-ci.org/snemmani/de-challenge)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=snemmani_de-challenge&metric=alert_status)](https://sonarcloud.io/dashboard?id=snemmani_de-challenge) 
# Inverted index creation on a large collection of documents

## Problem Statement
Create an efficient implementation to build an inverted index of a large collection of documents.
Detailed problem statement can be found [here](./problem-statement.pdf).

## Tech Stack used
Python<br>
Spark

## Execution
1. Configure runtime settings
    1. Modify $PROJECT_ROOT/settings.py and fill in the correct dataset location, 
    2. Modify SparkConf within the same file

2. Create environment variables for execution

    * `export JAVA_HOME=<JAVA_HOME>`
    * `export SPARK_HOME=<SPARK_HOME>`
    * `export HADOOP_HOME=<HADOOP_HOME>`
    * `export PATH=$SPARK_HOME/bin:$PATH`
    * `export PYSPARK_PYTHON=python3`
    * `export SPARK_MASTER=<spark_master> # Only if there is a master node configured, else default will be local`
    
3. Submit the job
    `spark-submit app.py`

4. Result file for the dictionary will be
    `<dataset_locaiton>/id_files/dictionary`

4. Result file for the inverted index will be
    `<dataset_locaiton>/id_files/inverted-index`
