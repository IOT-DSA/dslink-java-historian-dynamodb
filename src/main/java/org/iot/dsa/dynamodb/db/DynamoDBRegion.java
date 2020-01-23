package org.iot.dsa.dynamodb.db;

import org.iot.dsa.dynamodb.Main;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;

public class DynamoDBRegion {
	
	private AmazonDynamoDB client;// = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_WEST_1).withCredentials(Main.getInstance()).build();
    private DynamoDB dynamoDB;
    private Regions region;
    
    public DynamoDBRegion(Regions region) {
        this(region, null);
    }
    
    public DynamoDBRegion(Regions region, String endpoint) {
    	this.region = region;
    	AmazonDynamoDBClientBuilder clientbuilder = AmazonDynamoDBClientBuilder.standard().withCredentials(Main.getInstance());
    	if (endpoint == null || endpoint.isEmpty()) {
    	    clientbuilder.withRegion(region);
    	} else {
    	    AwsClientBuilder.EndpointConfiguration endpointConfig = new AwsClientBuilder.EndpointConfiguration(endpoint, region.getName());
    	    clientbuilder.withEndpointConfiguration(endpointConfig);
    	}
    	client = clientbuilder.build();
    	
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
