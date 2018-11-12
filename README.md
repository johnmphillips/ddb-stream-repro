start localstack with 

```
docker run -e SERVICES=kinesis,dynamodb,dynamodbstreams -e USE_SSL=true -e DEFAULT_REGION=eu-west-1 -p4569:4569 -p4568:4568 -p4570:4570 localstack/localstack
```

start the application with

```
mvn package && java -jar target/ddb-stream-repro-1.0-SNAPSHOT.jar
```