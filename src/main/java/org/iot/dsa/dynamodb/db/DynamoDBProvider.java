package org.iot.dsa.dynamodb.db;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.actions.ActionResult;
import org.dsa.iot.dslink.node.actions.Parameter;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.dslink.util.handler.Handler;
import org.dsa.iot.historian.database.Database;
import org.dsa.iot.historian.database.DatabaseProvider;
import org.dsa.iot.historian.database.Watch;
import org.iot.dsa.dynamodb.Main;
import org.iot.dsa.dynamodb.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;

public class DynamoDBProvider extends DatabaseProvider {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBProvider.class);
	
	private AmazonDynamoDB client;// = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_WEST_1).withCredentials(Main.getInstance()).build();
    private DynamoDB dynamoDB;// = new DynamoDB(client);
//	private final DynamoDBPurger purger = new DynamoDBPurger();

    public DynamoDBProvider() {
//        purger.setupPurger();
    	client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_WEST_1).withCredentials(Main.getInstance()).build();
    	dynamoDB = new DynamoDB(client);
    }

//    public DynamoDBPurger getPurger() {
//        return purger;
//    }

	public void stop() {
//		purger.stop;
	}

	@Override
	public Action createDbAction(Permission perm) {
		Action act = new Action(perm, new Handler<ActionResult>() {
			@Override
			public void handle(ActionResult event) {
				String tableName = event.getParameter(Util.EXISTING_TABLE_NAME).getString();
				if (Util.NEW_TABLE_OPTION.equals(tableName)) {
					tableName = event.getParameter(Util.NEW_TABLE_NAME, ValueType.STRING).getString();
					createTable(tableName);
				}
				if (tableName != null) {
					NodeBuilder builder = createDbNode(tableName, event);
					createAndInitDb(builder.build());
				}
			}
		});
		List<String> dropdown = new LinkedList<String>();
        dropdown.add(Util.NEW_TABLE_OPTION);
        
        try {
			TableCollection<ListTablesResult> tables = dynamoDB.listTables();
	        Iterator<Table> iterator = tables.iterator();
	        while (iterator.hasNext()) {
	            Table table = iterator.next();
	            dropdown.add(table.getTableName());
	        }
        } catch (Exception e) {
        	LOGGER.warn("", e);
        }
		act.addParameter(new Parameter(Util.EXISTING_TABLE_NAME, ValueType.makeEnum(dropdown), new Value(Util.NEW_TABLE_OPTION)));
		act.addParameter(new Parameter(Util.NEW_TABLE_NAME, ValueType.STRING).setDescription("Only applicable when creating a new table"));
		return act;
	}
	
	private void createTable(String tableName) {
		try {
            List<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
            attributeDefinitions.add(new AttributeDefinition().withAttributeName(Util.WATCH_PATH_KEY).withAttributeType("S"));
            attributeDefinitions.add(new AttributeDefinition().withAttributeName(Util.TS_KEY).withAttributeType("N"));
//            attributeDefinitions.add(new AttributeDefinition().withAttributeName(Util.VALUE).withAttributeType("S"));
            
            List<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
            keySchema.add(new KeySchemaElement().withAttributeName(Util.WATCH_PATH_KEY).withKeyType(KeyType.HASH));
            keySchema.add(new KeySchemaElement().withAttributeName(Util.TS_KEY).withKeyType(KeyType.RANGE));

            CreateTableRequest request = new CreateTableRequest()
            		.withTableName(tableName)
            		.withKeySchema(keySchema)
            		.withAttributeDefinitions(attributeDefinitions)
            		.withProvisionedThroughput(new ProvisionedThroughput()
            				.withReadCapacityUnits(5L).withWriteCapacityUnits(6L));
            Table table = dynamoDB.createTable(request);
            table.waitForActive();
        }
        catch (Exception e) {
            LOGGER.error("CreateTable request failed for " + tableName);
            LOGGER.error(e.getMessage());
        }
	}

	@Override
	protected Database createDb(Node node) {
		String tableName = node.getName();
		DynamoDBMapperConfig mapperConfig = DynamoDBMapperConfig.builder()
				.withTableNameOverride(TableNameOverride.withTableNameReplacement(tableName))
				.build();
		DynamoDBMapper mapper = new DynamoDBMapper(client, mapperConfig);
		return new DynamoDBProxy(tableName, this, mapper);
	}

	@Override
	public Permission dbPermission() {
		return Permission.CONFIG;
	}

	@Override
	public void deleteRange(Watch watch, long fromTs, long toTs) {
		// TODO Auto-generated method stub

	}
}
