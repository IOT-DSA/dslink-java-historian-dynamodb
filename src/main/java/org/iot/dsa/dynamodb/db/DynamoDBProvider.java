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
import org.dsa.iot.dslink.node.actions.EditorType;
import org.dsa.iot.dslink.node.actions.Parameter;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.dslink.util.handler.Handler;
import org.dsa.iot.historian.database.Database;
import org.dsa.iot.historian.database.DatabaseProvider;
import org.dsa.iot.historian.database.Watch;
import org.dsa.iot.historian.utils.TimeParser;
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
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTimeToLiveRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTimeToLiveResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TimeToLiveDescription;
import com.amazonaws.services.dynamodbv2.model.TimeToLiveSpecification;
import com.amazonaws.services.dynamodbv2.model.UpdateTimeToLiveRequest;

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
					long rcu = event.getParameter(Util.NEW_TABLE_RCU, ValueType.NUMBER).getNumber().longValue();
					long wcu = event.getParameter(Util.NEW_TABLE_WCU, ValueType.NUMBER).getNumber().longValue();
					try {
						createTable(tableName, rcu, wcu);
					} catch (Exception e) {
						LOGGER.error("CreateTable request failed for " + tableName);
			            LOGGER.error(e.getMessage());
			            tableName = null;
					}
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
		act.addParameter(new Parameter(Util.NEW_TABLE_RCU, ValueType.NUMBER, new Value(5L)));
		act.addParameter(new Parameter(Util.NEW_TABLE_WCU, ValueType.NUMBER, new Value(6L)));
		return act;
	}
	
	private void createTable(String tableName, long rcu, long wcu) throws Exception {
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
        				.withReadCapacityUnits(rcu).withWriteCapacityUnits(wcu));
        Table table = dynamoDB.createTable(request);
        table.waitForActive();
	}
	
	TableDescription getTableInfo(String tableName) {
		Table table = dynamoDB.getTable(tableName);
		return table.describe();
	}
	
	void updateTTL(String tableName, boolean enabled) {
		TimeToLiveSpecification ttlSpec = new TimeToLiveSpecification()
        		.withAttributeName(Util.TTL)
        		.withEnabled(enabled);
		UpdateTimeToLiveRequest ttlReq = new UpdateTimeToLiveRequest()
        		.withTableName(tableName)
        		.withTimeToLiveSpecification(ttlSpec);
		try {
			client.updateTimeToLive(ttlReq);
		} catch (AmazonDynamoDBException e) {
			LOGGER.debug("" ,e);
		}
	}
	
	TimeToLiveDescription getTTLInfo(String tableName) {
		DescribeTimeToLiveRequest getTTLReq = new DescribeTimeToLiveRequest()
				.withTableName(tableName);
		DescribeTimeToLiveResult result = client.describeTimeToLive(getTTLReq);
		return result.getTimeToLiveDescription();
	}
	
	void updateTable(String tableName, long rcu, long wcu) {
		try {
			Table table = dynamoDB.getTable(tableName);
			table.updateTable(new ProvisionedThroughput().withReadCapacityUnits(rcu).withWriteCapacityUnits(wcu));
			table.waitForActive();
		} catch (Exception e) {
			LOGGER.error("UpdateTable request failed for " + tableName);
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
	public void onWatchAdded(final Watch watch) {
		final Node node = watch.getNode();
        final Database database = watch.getGroup().getDb();
        final Permission perm = database.getProvider().dbPermission();
        NodeBuilder b = node.createChild("unsubPurge", true).setDisplayName("Unsubscribe and Purge");
        Action a = new Action(perm, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                watch.unsubscribe();
                
                String path = watch.getPath();
                DynamoDBProxy db = (DynamoDBProxy) watch.getGroup().getDb();
                db.delete(path, -1, -1);
            }
        });
        b.setAction(a);
        b.build();
        
        b = node.createChild("purge", true).setDisplayName("Purge");
        a = new Action(perm, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                long fromTs = -1;
                long toTs = -1;

                Value vTR = event.getParameter("Timerange");
                if (vTR != null) {
                    String[] split = vTR.getString().split("/");
                    fromTs = TimeParser.parse(split[0]);
                    toTs = TimeParser.parse(split[1]);
                }
                DynamoDBProxy db = (DynamoDBProxy) watch.getGroup().getDb();
        		db.delete(watch.getPath(), fromTs, toTs);
            }
        });
        a.addParameter(new Parameter("Timerange", ValueType.STRING).setEditorType(EditorType.DATE_RANGE).setDescription("The range for which to purge data"));
        b.setAction(a);
        b.build();
	}

	@Override
	public void deleteRange(Watch watch, long fromTs, long toTs) {
		DynamoDBProxy db = (DynamoDBProxy) watch.getGroup().getDb();
		db.delete(watch.getPath(), fromTs, toTs);
	}
}
