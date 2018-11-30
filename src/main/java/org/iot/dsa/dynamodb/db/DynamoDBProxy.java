package org.iot.dsa.dynamodb.db;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.Writable;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.actions.ActionResult;
import org.dsa.iot.dslink.node.actions.Parameter;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValuePair;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.dslink.util.Objects;
import org.dsa.iot.dslink.util.TimeUtils;
import org.dsa.iot.dslink.util.handler.CompleteHandler;
import org.dsa.iot.dslink.util.handler.Handler;
import org.dsa.iot.dslink.util.json.JsonArray;
import org.dsa.iot.dslink.util.json.JsonObject;
import org.dsa.iot.historian.database.Database;
import org.dsa.iot.historian.utils.QueryData;
import org.iot.dsa.dynamodb.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TimeToLiveDescription;

public class DynamoDBProxy extends Database {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBProxy.class);
    private final DynamoDBProvider provider;
    private final DynamoDBMapper mapper;
    ScheduledFuture<?> tableInfoPoller;
    
    private Node attrDefsNode;
    private Node creationNode;
    private Node gsisNode;
	private Node itemCountNode;
	private Node keySchemaNode;
	private Node streamArnNode;
	private Node provThroughputNode;
	private Node lsisNode;
	private Node streamLabelNode;
	private Node streamSpecNode;
	private Node tableArnNode;
	private Node tableNameNode;
	private Node tableSizeNode;
	private Node tableStatusNode;
	private Node ttlEnabledNode;
	private Node ttlDefaultDaysNode;
	private Node ttlStatusNode;
 
	
	public DynamoDBProxy(String name, DynamoDBProvider provider, DynamoDBMapper mapper) {
        super(name, provider);
        this.mapper = mapper;
        this.provider = provider;
    }
	
	void setTTLEnabled(String tableName, boolean enabled) {
		provider.updateTTL(tableName, enabled);
		refreshTTLStatus(tableName);
	}
	
	public long getExpiration() {
		return getExpiration(System.currentTimeMillis());
	}
	
	public long getExpiration(long now) {
		long ttl = (long) (ttlDefaultDaysNode.getValue().getNumber().doubleValue() * 24 * 60 * 60 * 1000);
		return now + ttl;
	}
	
	public void batchWrite(Iterable<DBEntry> entries) {
		mapper.batchSave(entries);
	}
	
	public void write(String path, Value value, long ts, long expiration) {
		DBEntry entry = new DBEntry();
		entry.setWatchPath(path);
		entry.setTs(ts);
		entry.setValue(value.toString());
		entry.setExpiration(expiration);

		mapper.save(entry);
	}

	@Override
	public void write(String path, Value value, long ts) {
		write(path, value, ts, getExpiration());
	}

	@Override
	public void query(String path, long from, long to, CompleteHandler<QueryData> handler) {
		Map<String, AttributeValue> eav = new HashMap<String, AttributeValue>();
		eav.put(":v1", new AttributeValue().withS(path));
		eav.put(":v2", new AttributeValue().withN(String.valueOf(from)));
		eav.put(":v3", new AttributeValue().withN(String.valueOf(to)));

		DynamoDBQueryExpression<DBEntry> queryExpression = new DynamoDBQueryExpression<DBEntry>() 
		    .withKeyConditionExpression(Util.WATCH_PATH_KEY + " = :v1 and " + Util.TS_KEY + " between :v2 and :v3")
		    .withExpressionAttributeValues(eav);
		List<DBEntry> results = mapper.query(DBEntry.class, queryExpression);
		
		for (DBEntry result: results) {
			handler.handle(new QueryData(new Value(result.getValue()), result.getTs()));
		}
		handler.complete();
	}

	@Override
	public QueryData queryFirst(String path) {
		Map<String, AttributeValue> eav = new HashMap<String, AttributeValue>();
		eav.put(":v1", new AttributeValue().withS(path));

		DynamoDBQueryExpression<DBEntry> queryExpression = new DynamoDBQueryExpression<DBEntry>() 
		    .withKeyConditionExpression(Util.WATCH_PATH_KEY + " = :v1")
		    .withExpressionAttributeValues(eav)
		    .withLimit(1);
		List<DBEntry> results = mapper.query(DBEntry.class, queryExpression);
		
		DBEntry first = results.isEmpty() ? null : results.get(0);
		return first == null ? null : new QueryData(new Value(first.getValue()), first.getTs());
	}

	@Override
	public QueryData queryLast(String path) {
		Map<String, AttributeValue> eav = new HashMap<String, AttributeValue>();
		eav.put(":v1", new AttributeValue().withS(path));

		DynamoDBQueryExpression<DBEntry> queryExpression = new DynamoDBQueryExpression<DBEntry>() 
		    .withKeyConditionExpression(Util.WATCH_PATH_KEY + " = :v1")
		    .withExpressionAttributeValues(eav)
		    .withLimit(1)
		    .withScanIndexForward(false);
		List<DBEntry> results = mapper.query(DBEntry.class, queryExpression);
		
		DBEntry first = results.isEmpty() ? null : results.get(0);
		return first == null ? null : new QueryData(new Value(first.getValue()), first.getTs());
	}
	
	public void delete(String path, long fromTs, long toTs) {
		
		Map<String, AttributeValue> eav = new HashMap<String, AttributeValue>();
		eav.put(":v1", new AttributeValue().withS(path));
		if (fromTs >= 0) {
			eav.put(":v2", new AttributeValue().withN(String.valueOf(fromTs)));
		}
		if (toTs >= 0) {
			eav.put(":v3", new AttributeValue().withN(String.valueOf(toTs)));
		}
		String cond = Util.WATCH_PATH_KEY + " = :v1";
		if (fromTs >= 0 && toTs >= 0) {
			cond += " and " + Util.TS_KEY + " between :v2 and :v3";
		} else if (fromTs >= 0) {
			cond += " and " + Util.TS_KEY + " > :v2";
		} else if (toTs >= 0) {
			cond += " and " + Util.TS_KEY + " < :v3";
		}
		

		DynamoDBQueryExpression<DBEntry> queryExpression = new DynamoDBQueryExpression<DBEntry>() 
		    .withKeyConditionExpression(cond)
		    .withExpressionAttributeValues(eav);
		List<DBEntry> results = mapper.query(DBEntry.class, queryExpression);
		
		mapper.batchDelete(results);
	}

	@Override
	public void close() throws Exception {
		if (tableInfoPoller != null) {
			tableInfoPoller.cancel(true);
		}
	}

	@Override
	protected void performConnect() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void initExtensions(final Node node) {
		ScheduledThreadPoolExecutor stpe = Objects.getDaemonThreadPool();
		
		attrDefsNode = node.createChild(Util.ATTR_DEFINITIONS, true).setValueType(ValueType.ARRAY).build();
		attrDefsNode.setSerializable(false);
		creationNode = node.createChild(Util.CREATION_DATETIME, true).setValueType(ValueType.STRING).build();
		creationNode.setSerializable(false);
		gsisNode = node.createChild(Util.GLOBAL_SECONDARY_INDICES, true).setValueType(ValueType.ARRAY).build();
		gsisNode.setSerializable(false);
		itemCountNode = node.createChild(Util.ITEM_COUNT, true).setValueType(ValueType.NUMBER).build();
		itemCountNode.setSerializable(false);
		keySchemaNode = node.createChild(Util.KEY_SCHEMA, true).setValueType(ValueType.ARRAY).build();
		keySchemaNode.setSerializable(false);
		streamArnNode = node.createChild(Util.STREAM_ARN, true).setValueType(ValueType.STRING).build();
		streamArnNode.setSerializable(false);
		streamLabelNode = node.createChild(Util.STREAM_LABEL, true).setValueType(ValueType.STRING).build();
		streamLabelNode.setSerializable(false);
		lsisNode = node.createChild(Util.LOCAL_SECONDARY_INDICES, true).setValueType(ValueType.ARRAY).build();
		lsisNode.setSerializable(false);
		provThroughputNode = node.createChild(Util.PROVISIONED_THROUGHPUT, true).setValueType(ValueType.MAP).build();
		provThroughputNode.setSerializable(false);
		streamSpecNode = node.createChild(Util.STREAM_SPEC, true).setValueType(ValueType.STRING).build();
		streamSpecNode.setSerializable(false);
		tableArnNode = node.createChild(Util.TABLE_ARN, true).setValueType(ValueType.STRING).build();
		tableArnNode.setSerializable(false);
		tableNameNode = node.createChild(Util.TABLE_NAME, true).setValueType(ValueType.STRING).build();
		tableNameNode.setSerializable(false);
		tableSizeNode = node.createChild(Util.TABLE_SIZE_BYTES, true).setValueType(ValueType.NUMBER).build();
		tableSizeNode.setSerializable(false);
		tableStatusNode = node.createChild(Util.TABLE_STATUS, true).setValueType(ValueType.STRING).build();
		tableStatusNode.setSerializable(false);
        ttlStatusNode = node.createChild(Util.TTL_STATUS, true).setValueType(ValueType.STRING).build();
        ttlStatusNode.setSerializable(false);
        ttlEnabledNode = node.getChild(Util.TTL_ENABLED, true);
        if (ttlEnabledNode == null) {
        	ttlEnabledNode = node.createChild(Util.TTL_ENABLED, true).setValueType(ValueType.BOOL).setValue(new Value(false)).build();
        } else if (ttlEnabledNode.getValue() != null){
        	stpe.schedule(new Runnable() {
				@Override
				public void run() {
					setTTLEnabled(node.getName(), ttlEnabledNode.getValue().getBool());
				}
			}, 0, TimeUnit.MILLISECONDS);
        }
        ttlEnabledNode.getListener().setValueHandler(new Handler<ValuePair>() {
			@Override
			public void handle(ValuePair event) {
				if (event.isFromExternalSource()) {
					setTTLEnabled(node.getName(), event.getCurrent().getBool());
				}
			}      	
        });
        ttlEnabledNode.setWritable(Writable.WRITE);
        ttlDefaultDaysNode = node.getChild(Util.TTL_DEFAULT, true);
        if (ttlDefaultDaysNode == null) {
        	ttlDefaultDaysNode = node.createChild(Util.TTL_DEFAULT, true).setValueType(ValueType.NUMBER).setValue(new Value(1461)).build();
        }
        ttlDefaultDaysNode.setWritable(Writable.WRITE);
		tableInfoPoller = stpe.scheduleWithFixedDelay(new Runnable() {
			@Override
			public void run() {
				refreshTableDetails(node);
			}		
		}, 0, 6, TimeUnit.HOURS);
	}

	protected void refreshTableDetails(final Node node) {
		TableDescription tableInfo = provider.getTableInfo(node.getName());
		
		List<AttributeDefinition> attrDefs = tableInfo.getAttributeDefinitions();
		if (attrDefs != null) {
			JsonArray ja = new JsonArray();
			for (AttributeDefinition ad: attrDefs) {
				ja.add(ad.toString());
			}
			attrDefsNode.setValue(new Value(ja));
		}
		
		Date creation = tableInfo.getCreationDateTime();
		if (creation != null) {
			creationNode.setValue(new Value(TimeUtils.format(creation)));
		}
		
		List<GlobalSecondaryIndexDescription> gsis = tableInfo.getGlobalSecondaryIndexes();
		if (gsis != null) {
			JsonArray ja = new JsonArray();
			for (GlobalSecondaryIndexDescription gsi: gsis) {
				ja.add(gsi.toString());
			}
			gsisNode.setValue(new Value(ja));
		}
		
		long itemCount = tableInfo.getItemCount();
		itemCountNode.setValue(new Value(itemCount));
		
		List<KeySchemaElement> keySchema = tableInfo.getKeySchema();
		if (keySchema != null) {
			JsonArray ja = new JsonArray();
			for (KeySchemaElement elem: keySchema) {
				ja.add(elem.toString());
			}
			keySchemaNode.setValue(new Value(ja));
		}
		
		String latestStreamArn = tableInfo.getLatestStreamArn();
		if (latestStreamArn != null) {
			streamArnNode.setValue(new Value(latestStreamArn));
		}
		
		String latestStreamLabel = tableInfo.getLatestStreamLabel();
		if (latestStreamLabel != null) {
			streamLabelNode.setValue(new Value(latestStreamLabel));
		}
		
		List<LocalSecondaryIndexDescription> lsis = tableInfo.getLocalSecondaryIndexes();
		if (lsis != null) {
			JsonArray ja = new JsonArray();
			for (LocalSecondaryIndexDescription lsi: lsis) {
				ja.add(lsi.toString());
			}
			lsisNode.setValue(new Value(ja));
		}
		
		ProvisionedThroughputDescription provThru = tableInfo.getProvisionedThroughput();
		if (provThru != null) {
			long latestRCU = provThru.getReadCapacityUnits();
			long latestWCU = provThru.getWriteCapacityUnits();
			Date lastDecrease = provThru.getLastDecreaseDateTime();
			Date lastIncrease = provThru.getLastIncreaseDateTime();
			long numDecreases = provThru.getNumberOfDecreasesToday();
			JsonObject jo = new JsonObject();
			jo.put("Read Capacity Units", latestRCU);
			jo.put("Write Capacity Units", latestWCU);
			jo.put("Last Decrease Date", lastDecrease != null ? TimeUtils.format(lastDecrease) : null);
			jo.put("Last Increase Date", lastIncrease != null ? TimeUtils.format(lastIncrease) : null);
			jo.put("Number of Decreases Today", numDecreases);
			provThroughputNode.setValue(new Value(jo));
			
			Action act = new Action(Permission.READ, new Handler<ActionResult>() {
				@Override
				public void handle(ActionResult event) {
					long rcu = event.getParameter(Util.RCU, ValueType.NUMBER).getNumber().longValue();
					long wcu = event.getParameter(Util.WCU, ValueType.NUMBER).getNumber().longValue();
					provider.updateTable(node.getName(), rcu, wcu);
					refreshTableDetails(node);
				}
			});
			act.addParameter(new Parameter(Util.RCU, ValueType.NUMBER, new Value(latestRCU)));
			act.addParameter(new Parameter(Util.WCU, ValueType.NUMBER, new Value(latestWCU)));
			Node anode = node.getChild(Util.EDIT_TABLE, true);
			if (anode == null) {
				node.createChild(Util.EDIT_TABLE, true).setAction(act).build().setSerializable(false);
			} else {
				anode.setAction(act);
			}
		}
		
		StreamSpecification streamSpec = tableInfo.getStreamSpecification();
		if (streamSpec != null) {
			streamSpecNode.setValue(new Value(streamSpec.toString()));
		}
		
		String tableArn = tableInfo.getTableArn();
		if (tableArn != null) {
			tableArnNode.setValue(new Value(tableArn));
		}
		
		String tableName = tableInfo.getTableName();
		if (tableName != null) {
			tableNameNode.setValue(new Value(tableName));
		}
		
		long tableBytes = tableInfo.getTableSizeBytes();
		tableSizeNode.setValue(new Value(tableBytes));
		
		String tableStatus = tableInfo.getTableStatus();
		if (tableStatus != null) {
			tableStatusNode.setValue(new Value(tableStatus));
		}
		
		refreshTTLStatus(tableName);
	}
	
	private void refreshTTLStatus(String tableName) {
		TimeToLiveDescription ttlDesc = provider.getTTLInfo(tableName);
		if (ttlDesc != null) {
			String ttlStatus = ttlDesc.getTimeToLiveStatus();
			if (ttlStatus != null) {
				ttlStatusNode.setValue(new Value(ttlStatus));
			}
		}
	}
}
