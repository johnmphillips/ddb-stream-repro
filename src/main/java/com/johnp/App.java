package com.johnp;

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;

import java.util.concurrent.Executors;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true");
        System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");

        AWSStaticCredentialsProvider creds = new AWSStaticCredentialsProvider(new BasicAWSCredentials("hello", "world"));

        AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(creds)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        "https://localhost:4569",
                        "eu-west-1"
                ))
                .build();

        AmazonDynamoDBStreams streamsClient = AmazonDynamoDBStreamsClientBuilder.standard()
                .withCredentials(creds)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        "https://localhost:4570",
                        "eu-west-1"
                ))
                .build();

        System.out.println("Creating table... ");
        CreateTableResult createTableResult = ddb.createTable(new CreateTableRequest()
                .withTableName("my-table")
                .withProvisionedThroughput(new ProvisionedThroughput(10L, 10L))
                .withKeySchema(new KeySchemaElement("myHashKey", KeyType.HASH))
                .withAttributeDefinitions(new AttributeDefinition("myHashKey", ScalarAttributeType.S))
                .withStreamSpecification(new StreamSpecification()
                        .withStreamEnabled(true)
                        .withStreamViewType(StreamViewType.NEW_IMAGE))
        );

        String streamArn = createTableResult.getTableDescription().getLatestStreamArn();

        System.out.println("Creating worker for dynamo stream " + streamArn);

        KinesisClientLibConfiguration workerConfig = new KinesisClientLibConfiguration(
                "my-app",
                streamArn,
                creds,
                "worker1");

        Worker worker = new Worker.Builder()
                .kinesisClient(new AmazonDynamoDBStreamsAdapterClient(streamsClient))
                .dynamoDBClient(ddb)
                .config(workerConfig)
                .recordProcessorFactory(RecordProcessor::new)
                .metricsFactory(new NullMetricsFactory())
                .build();

        System.out.println("Starting worker for dynamo stream " + streamArn);

        Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()).submit(worker);
    }

    private static class RecordProcessor implements IRecordProcessor {

        public void initialize(InitializationInput initializationInput) {

        }

        public void processRecords(ProcessRecordsInput processRecordsInput) {

        }

        public void shutdown(ShutdownInput shutdownInput) {

        }
    }
}
