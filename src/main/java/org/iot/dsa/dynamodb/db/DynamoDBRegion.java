package org.iot.dsa.dynamodb.db;

import org.iot.dsa.dynamodb.Main;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;

public class DynamoDBRegion {
	
	private AmazonDynamoDB client;// = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_WEST_1).withCredentials(Main.getInstance()).build();
    private DynamoDB dynamoDB;
    private Regions region;
    
    public DynamoDBRegion(Regions region) {
    	this.region = region;
    	client = AmazonDynamoDBClientBuilder.standard().withRegion(region).withCredentials(Main.getInstance()).build();
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
