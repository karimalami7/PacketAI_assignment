# PacketAI_assignment

### To run problem 1

```shell 
$SPARK_HOME/bin/spark-submit src/problem1.py gutenberg-sample
```

### To run problem 2 (requires mongodb)

```shell 
$SPARK_HOME/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.2 src/problem2.py gutenberg-sample
```

### To run problem 3 

```shell
python3 problem3.py <host> <port> <database> <collection>
```