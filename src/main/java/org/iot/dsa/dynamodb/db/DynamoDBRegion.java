package org.iot.dsa.dynamodb.db;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import org.iot.dsa.dynamodb.Main;

public class DynamoDBRegion {

    private final AmazonDynamoDB client;
    private final DynamoDB dynamoDB;
    private final Regions region;

    public DynamoDBRegion(Regions region) {
        this.region = region;
        client = AmazonDynamoDBClientBuilder.standard().withRegion(region)
                                            .withCredentials(Main.getInstance()).build();
        dynamoDB = new DynamoDB(client);
    }

    public AmazonDynamoDB getClient() {
        return client;
    }

    public DynamoDB getDynamoDB() {
        return dynamoDB;
    }

    public Regions getRegion() {
        return region;
    }
}
