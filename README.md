# Advanced Topics in Database Systems

My solutions to coursework of the advanced topics in database systems course @ NTUA.

## Introduction

The assignment is about using PySpark and hadoop in a cluster. More specifically we are asked to download and process trip record data (yellow taxi) from the NYC Taxi and Limousine Commission. Once we upload the data to the hadoop cluster we process them using PySpark using the PySpark python client.

## Requirements

Firstly you need to set up a [Apache Hadoop](https://hadoop.apache.org/) and [Apache PySpark](https://spark.apache.org/docs/latest/api/python/) cluster. Also you need python 3.10 preferably. My preference is to use it by installing [miniconda](https://docs.conda.io/en/latest/miniconda.html) and creating a virtual environment.

## Running the scripts

You can run the scripts by using the following command:

```bash
$SPARK_PATH/bin/spark-submit --master spark://master:7077 <script.py>
```

where `$SPARK_PATH` is where the spark folder is, `master` is the main/ master machine of the cluster.
