package org.iot.dsa.dynamodb.db;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.util.handler.CompleteHandler;
import org.dsa.iot.historian.database.Database;
import org.dsa.iot.historian.utils.QueryData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;

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
		// TODO Auto-generated method stub

	}

	@Override
	public QueryData queryFirst(String path) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public QueryData queryLast(String path) {
		// TODO Auto-generated method stub
		return null;
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
