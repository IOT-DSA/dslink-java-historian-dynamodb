package org.iot.dsa.dynamodb.db;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.util.handler.CompleteHandler;
import org.dsa.iot.historian.database.Database;
import org.dsa.iot.historian.utils.QueryData;
import org.iot.dsa.dynamodb.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

public class DynamoDBProxy extends Database {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBProxy.class);
    private final DynamoDBProvider provider;
    private final DynamoDBMapper mapper;
	
	public DynamoDBProxy(String name, DynamoDBProvider provider, DynamoDBMapper mapper) {
        super(name, provider);
        this.mapper = mapper;
        this.provider = provider;
    }

	@Override
	public void write(String path, Value value, long ts) {
		DBEntry entry = new DBEntry();
		entry.setWatchPath(path);
		entry.setTs(ts);
		entry.setValue(value.toString());

		mapper.save(entry);
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

	@Override
	public void close() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	protected void performConnect() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void initExtensions(Node node) {
		// TODO Auto-generated method stub

	}

}
