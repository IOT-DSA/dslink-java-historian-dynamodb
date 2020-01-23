package org.iot.dsa.dynamodb.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.actions.ActionResult;
import org.dsa.iot.dslink.node.actions.EditorType;
import org.dsa.iot.dslink.node.actions.Parameter;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.dslink.util.TimeUtils;
import org.dsa.iot.dslink.util.handler.Handler;
import org.dsa.iot.dslink.util.json.JsonArray;
import org.dsa.iot.historian.database.Database;
import org.dsa.iot.historian.database.DatabaseProvider;
import org.dsa.iot.historian.database.Watch;
import org.iot.dsa.dynamodb.Main;
import org.iot.dsa.dynamodb.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
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
	
	private Map<RegionEndpoint, DynamoDBRegion> regionMap = new HashMap<RegionEndpoint, DynamoDBRegion>();
	private final BufferPurger purger = new BufferPurger();

    public DynamoDBProvider() {
        purger.setupPurger();
    }
    
    private DynamoDBRegion getRegionObject(RegionEndpoint regionEndpoint) {
    	DynamoDBRegion regionObj = regionMap.get(regionEndpoint);
    	if (regionObj == null) {
    		regionObj = new DynamoDBRegion(regionEndpoint.getRegion(), regionEndpoint.getEndpoint());
    		regionMap.put(regionEndpoint, regionObj);
    	}
    	return regionObj;
    }
    
    private AmazonDynamoDB getClient(RegionEndpoint regionEndpoint) {
    	return getRegionObject(regionEndpoint).getClient();
    }
    
    private AmazonDynamoDB getClient(Regions region, String endpoint) {
        return getClient(new RegionEndpoint(region, endpoint));
    }
    
    private DynamoDB getDynamoDB(RegionEndpoint regionEndpoint) {
    	return getRegionObject(regionEndpoint).getDynamoDB();
    }
    
    private DynamoDB getDynamoDB(Regions region, String endpoint) {
        return getDynamoDB(new RegionEndpoint(region, endpoint));
    }
    
    private DynamoDB getDynamoDB() {
    	return getDynamoDB(Main.getInstance().getDefaultRegion());
    }

    public BufferPurger getPurger() {
        return purger;
    }

	public void stop() {
		purger.stop();
	}

	@Override
	public Action createDbAction(Permission perm) {
		Action act = new Action(perm, new Handler<ActionResult>() {
			@Override
			public void handle(ActionResult event) {
				String tableName = event.getParameter(Util.EXISTING_TABLE_NAME).getString();
				Regions region = Regions.fromName(event.getParameter(Util.REGION).getString());
				Value endpointv = event.getParameter(Util.ENDPOINT);
				String endpoint = endpointv == null ? null : endpointv.getString();
				if (Util.NEW_TABLE_OPTION.equals(tableName)) {
					tableName = event.getParameter(Util.NEW_TABLE_NAME, ValueType.STRING).getString();
					long rcu = event.getParameter(Util.NEW_TABLE_RCU, ValueType.NUMBER).getNumber().longValue();
					long wcu = event.getParameter(Util.NEW_TABLE_WCU, ValueType.NUMBER).getNumber().longValue();
					try {
						createTable(tableName, region, endpoint, rcu, wcu);
					} catch (Exception e) {
						LOGGER.error("CreateTable request failed for " + tableName);
			            LOGGER.error(e.getMessage());
			            tableName = null;
					}
				} else if (Util.OTHER_TABLE_OPTION.equals(tableName)) {
					tableName = event.getParameter(Util.NEW_TABLE_NAME, ValueType.STRING).getString();
				}
				if (tableName != null) {
					NodeBuilder builder = createDbNode(tableName, event);
					builder.setRoConfig(Util.REGION, new Value(region.getName()));
					builder.setRoConfig(Util.ENDPOINT, new Value(endpoint));
					createAndInitDb(builder.build());
				}
			}
		});
		List<String> dropdown = new LinkedList<String>();
        dropdown.add(Util.NEW_TABLE_OPTION);
        dropdown.add(Util.OTHER_TABLE_OPTION);
        
        try {
			TableCollection<ListTablesResult> tables = getDynamoDB().listTables();
	        Iterator<Table> iterator = tables.iterator();
	        while (iterator.hasNext()) {
	            Table table = iterator.next();
	            dropdown.add(table.getTableName());
	        }
        } catch (Exception e) {
        	LOGGER.warn("", e);
        }
        RegionEndpoint defreg = Main.getInstance().getDefaultRegion();
		act.addParameter(new Parameter(Util.EXISTING_TABLE_NAME, ValueType.makeEnum(dropdown), new Value(Util.NEW_TABLE_OPTION)));
		act.addParameter(new Parameter(Util.REGION, ValueType.makeEnum(Util.getRegionList()), new Value(defreg.getRegion().getName())));
		act.addParameter(new Parameter(Util.ENDPOINT, ValueType.STRING, new Value(defreg.getEndpoint())).setDescription("Optional"));
		act.addParameter(new Parameter(Util.NEW_TABLE_NAME, ValueType.STRING).setDescription("Only applicable when choosing 'Create new table' or 'Other table'"));
		act.addParameter(new Parameter(Util.NEW_TABLE_RCU, ValueType.NUMBER, new Value(5L)).setDescription("Only applicable when creating new table"));
		act.addParameter(new Parameter(Util.NEW_TABLE_WCU, ValueType.NUMBER, new Value(6L)).setDescription("Only applicable when creating new table"));
		return act;
	}
	
	private void createTable(String tableName, Regions region, String endpoint, long rcu, long wcu) throws Exception {
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
        Table table = getDynamoDB(region, endpoint).createTable(request);
        table.waitForActive();
	}
	
	TableDescription getTableInfo(String tableName, Regions region, String endpoint) {
		Table table = getDynamoDB(region, endpoint).getTable(tableName);
		return table.describe();
	}
	
	void updateTTL(String tableName, Regions region, String endpoint, boolean enabled) {
		TimeToLiveSpecification ttlSpec = new TimeToLiveSpecification()
        		.withAttributeName(Util.TTL)
        		.withEnabled(enabled);
		UpdateTimeToLiveRequest ttlReq = new UpdateTimeToLiveRequest()
        		.withTableName(tableName)
        		.withTimeToLiveSpecification(ttlSpec);
		try {
			getClient(region, endpoint).updateTimeToLive(ttlReq);
		} catch (AmazonDynamoDBException e) {
			LOGGER.debug("" ,e);
		}
	}
	
	TimeToLiveDescription getTTLInfo(String tableName, Regions region, String endpoint) {
		DescribeTimeToLiveRequest getTTLReq = new DescribeTimeToLiveRequest()
				.withTableName(tableName);
		DescribeTimeToLiveResult result = getClient(region, endpoint).describeTimeToLive(getTTLReq);
		return result.getTimeToLiveDescription();
	}
	
	void updateTable(String tableName, Regions region, String endpoint, long rcu, long wcu) {
		try {
			Table table = getDynamoDB(region, endpoint).getTable(tableName);
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
		Regions region = Util.getRegionFromNode(node);
		String endpoint = Util.getEndpointFromNode(node);
		DynamoDBMapper mapper = new DynamoDBMapper(getClient(region, endpoint), mapperConfig);
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
                    fromTs = TimeUtils.decode(split[0]);
                    toTs = TimeUtils.decode(split[1]);
                }
                DynamoDBProxy db = (DynamoDBProxy) watch.getGroup().getDb();
        		db.delete(watch.getPath(), fromTs, toTs);
            }
        });
        a.addParameter(new Parameter("Timerange", ValueType.STRING).setEditorType(EditorType.DATE_RANGE).setDescription("The range for which to purge data"));
        b.setAction(a);
        b.build();
        
        b = node.createChild("insert", true).setDisplayName("Insert Record");
        a = new Action(perm, new Handler<ActionResult>() {
			@Override
			public void handle(ActionResult event) {
				Value vTs = event.getParameter(Util.TS, ValueType.STRING);
				Value val = event.getParameter(Util.VALUE, ValueType.STRING);
				Value vExp = event.getParameter(Util.TTL, ValueType.STRING);
				if (vTs != null && val != null && vExp != null) {
					long ts = TimeUtils.decode(vTs.getString());
					long exp = TimeUtils.decode(vExp.getString());
					DynamoDBProxy db = (DynamoDBProxy) watch.getGroup().getDb();
					db.write(watch.getPath(), val, ts, exp);
				}
			}
		});
        DynamoDBProxy db = (DynamoDBProxy) watch.getGroup().getDb();
        a.addParameter(new Parameter(Util.TS, ValueType.STRING));
        a.addParameter(new Parameter(Util.VALUE, ValueType.STRING));
        a.addParameter(new Parameter(Util.TTL, ValueType.STRING, new Value(TimeUtils.format(db.getExpiration()))));
        b.setAction(a);
        b.build();
        
        b = node.createChild("bulkInsert", true).setDisplayName("Bulk Insert Records");
        a = new Action(perm, new Handler<ActionResult>() {
			@Override
			public void handle(ActionResult event) {
				DynamoDBProxy db = (DynamoDBProxy) watch.getGroup().getDb();
				JsonArray ja = event.getParameter("Records", ValueType.ARRAY).getArray();
				db.batchWrite(watch.getPath(), ja);
			}
		});
        a.addParameter(new Parameter("Records", ValueType.ARRAY));
        b.setAction(a);
        b.build();
	}

	@Override
	public void deleteRange(Watch watch, long fromTs, long toTs) {
		DynamoDBProxy db = (DynamoDBProxy) watch.getGroup().getDb();
		db.delete(watch.getPath(), fromTs, toTs);
	}
}
