
# PacketAI_assignment


Download Gutenberg project data and create a sample of X books

```shell 
curl -sSL https://raw.githubusercontent.com/RHDZMOTA/spark-wordcount/develop/gutenberg.sh | sh
mkdir gutenberg-sample && ls gutenberg/ | shuf -n {X} | xargs -I _ cp gutenberg/_ gutenberg-sample/_
```

Now change current directory to PackeAI_assignment

```shell 
cd PacketAI_assignment/
```

### Problem 1
To run spark job

```shell 
$SPARK_HOME/bin/spark-submit src/problem1.py path/to/gutenberg-sample
```

### Problem 2 

Problem 2 requires mongodb. Download mongo image and create a docker container

```shell
sudo docker pull mongo:latest
sudo docker run -d -p 27017:27017 mongo
```

Now run spark job

```shell 
$SPARK_HOME/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.2 src/problem2.py path/to/gutenberg-sample
```

To query results via mongo shell:

```shell
sudo apt install mongodb-clients
mongo localhost/gutenberg
db.words.find()
```


### Problem 3 
First, run problem 2 to populate database

To start the api server:
```shell
python3 problem3.py <host> <port> <database> <collection>
```
**Note**: In this setting:

 - host: 127.0.0.1
 - port: 27017
 - database: gutenberg
 - collection: words

 1. To get the following words of a given word "X"

Open browser at http://127.0.0.1/5000/gutenberg/predict/next?word="X"

 2.  To guess a following word of a given word "X"

Open browser at http://127.0.0.1/5000/gutenberg/predict/random?word="X"