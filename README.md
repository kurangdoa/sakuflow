# Setting Up Environment
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
export SPARK_HOME=/Users/rhyando/code/development/datasaku/spark
export PATH=$PATH:$SPARK_HOME/bin
export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
export PYSPARK_PYTHON=python
export PATH=$PATH:$JAVA_HOME/jre/bin
```
# enter venv
sudo chmod +x /Users/rhyando/code/development/datasaku/_venv/bin/activate
source /Users/rhyando/code/development/datasaku/_venv/bin/activate
pip install -r requirements.txt
source deactivate