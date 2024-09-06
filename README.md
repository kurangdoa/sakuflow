# Introduction

Sakuflow is a data pipelines or data flow project that will be integrated with other overall datasaku infrastructure mentioned in this diagram link. The structure of the directory would be:

```
sakuflow
│   README.MD
│   requirements.txt    
│   
└───project_a
│   │   README.MD
│   │   config.yaml
│   │   
│   └───input
│   │       input.py
│   │       
│   └───staging
│   |       staging.py
│   |       
│   └───output
│   |       output.py
│   |       
|
└───project_b
|
└───project_c

```

# Project Structure
Every project will be located on each folder:

- input = processing of an data from outside resource to the bucket for the project.
- staging = after the input, data will be post-processed such as transformation or data quality check.
- output = the cleaned data after staging environment will be further flowed into respective datalake or datawarehouse.

Not every step is mandatory and there will be project with one or two steps only.

For very special project, the structure might be completely different.

# enter venv
sudo chmod +x /Users/rhyando/code/development/datasaku/_venv/bin/activate
source /Users/rhyando/code/development/datasaku/_venv/bin/activate
pip install -r requirements.txt
source deactivate

# special if working with vscode
{
    // Use IntelliSense to learn about possible attributes.
    // Hover to view descriptions of existing attributes.
    // For more information, visit: https://go.microsoft.com/fwlink/?linkid=830387
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Python Debugger: Current File",
            "type": "debugpy",
            "request": "launch",
            "program": "${file}",
            "console": "integratedTerminal",
            "cwd": "${fileDirname}",
            "env": {"PYTHONPATH": "/data/datasaku/sakuflow"}
        }
    ]
}

# test dagster

dagster is installed into the sakuflow and in order to test the pipeline locally, 
we could run this command.

```
dagster dev -w workspace.yaml
```


# archive

## Setting Up Environment
based on reneshbedre.com/blog/ttest-from-scratch.html

```
export SPARK_VERSION=3.5.1
export HADOOP_VERSION=3.3.4
export AWS_SDK_VERSION=1.12.262
cd $HOME
curl -L http://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz | tar -zx 
mv spark-${SPARK_VERSION}-bin-hadoop3 spark 
cd $HOME/spark/jars
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar
wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.0/iceberg-spark-runtime-3.5_2.12-1.5.0.jar
wget https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.2/postgresql-42.6.2.jar
wget https://repo.maven.apache.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.82.0/nessie-spark-extensions-3.5_2.12-0.82.0.jar
```

put below into .zshrc or .bashrc

```
export SPARK_HOME=/Users/rhyando/spark
export PATH=$PATH:$SPARK_HOME/bin
export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
export PYSPARK_PYTHON=python
export PATH=$PATH:$JAVA_HOME/jre/bin
```