package org.iot.dsa.dynamodb.db;

import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
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
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
import org.dsa.iot.etsdb.serializer.ByteData;
import org.dsa.iot.etsdb.serializer.ValueSerializer;
import org.dsa.iot.historian.database.Database;
import org.dsa.iot.historian.utils.QueryData;
import org.etsdb.DatabaseFactory;
import org.etsdb.QueryCallback;
import org.etsdb.impl.DatabaseImpl;
import org.iot.dsa.dynamodb.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamoDBProxy extends Database implements PurgeSettings {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBProxy.class);

    private Node attrDefsNode;
    private Node batchInterval;
    private Node batchSize;
    private DatabaseImpl<ByteData> buffer = null;
    private Node bufferMaxSizeNode;
    private Node bufferPathNode;
    private Node bufferPurgeEnabledNode;
    private Node creationNode;
    private Node gsisNode;
    private boolean initialized = false;
    private Node itemCountNode;
    private Node keySchemaNode;
    private Node lsisNode;
    private final DynamoDBMapper mapper;
    private Node myNode;
    private final String name;
    private Node provThroughputNode;
    private final DynamoDBProvider provider;
    private Node siteNameNode;
    private Node streamArnNode;
    private Node streamLabelNode;
    private Node streamSpecNode;
    private Node tableArnNode;
    ScheduledFuture<?> tableInfoPoller;
    private Node tableNameNode;
    private Node tableSizeNode;
    private Node tableStatusNode;
    private ScheduledThreadPoolExecutor threadPool = Objects.getDaemonThreadPool();
    private Node ttlDefaultDaysNode;
    private Node ttlEnabledNode;
    private Node ttlStatusNode;
    private boolean unsentInBuffer = true;
    private final List<Record> writeQueue = new LinkedList<>();
    private final WriteRunner writeRunner = new WriteRunner();
    private boolean writing = false;

    public DynamoDBProxy(Node node, DynamoDBProvider provider, DynamoDBMapper mapper) {
        super(node.getName(), provider);
        this.myNode = node;
        this.mapper = mapper;
        this.provider = provider;
        this.name = node.getName();

    }

    public boolean batchWrite(List<DBEntry> entries) {
        try {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("batchWrite records: " + entries.size());
            }
            mapper.batchSave(entries);
            return true;
        } catch (SdkClientException e) {
            LOGGER.debug("", e);
            return false;
        }
    }

    public void batchWrite(String path, JsonArray records) {
        List<DBEntry> entries = new ArrayList<DBEntry>();
        path = prependToPath(path);
        for (Object o : records) {
            DBEntry entry = Util.parseRecord(o);
            entry.setWatchPath(path);
            if (entry.getExpiration() == null) {
                entry.setExpiration(getExpiration());
            }
            entries.add(entry);
        }
        batchWrite(entries);
    }

    @Override
    public void close() throws Exception {
        if (tableInfoPoller != null) {
            tableInfoPoller.cancel(true);
        }
        writeToBuffer();
    }

    public void delete(String path, long fromTs, long toTs) {

        Map<String, AttributeValue> eav = new HashMap<String, AttributeValue>();
        eav.put(":v1", new AttributeValue().withS(prependToPath(path)));
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

    public long getExpiration() {
        return getExpiration(System.currentTimeMillis());
    }

    public long getExpiration(long now) {
        long ttl = (long) (ttlDefaultDaysNode.getValue().getNumber().doubleValue() * 24 * 60 * 60
                * 1000);
        return now + ttl;
    }

    @Override
    public long getMaxSizeInBytes() {
        return bufferMaxSizeNode.getValue().getNumber().longValue();
    }

    public String getSiteName() {
        if (siteNameNode == null) {
            return "";
        }
        return siteNameNode.getValue().getString();
    }

    public String getTableName() {
        return tableNameNode.getValue().getString();
    }

    @Override
    public void initExtensions(final Node node) {
        bufferPathNode = node.getChild(Util.BUFFER_PATH, true);
        if (bufferPathNode == null) {
            bufferPathNode = node.createChild(Util.BUFFER_PATH, true).setValueType(ValueType.STRING)
                                 .setValue(new Value("buffers/" + name)).build();
        }
        bufferPathNode.setWritable(Writable.WRITE);
        bufferPathNode.getListener().setValueHandler(new Handler<ValuePair>() {
            @Override
            public void handle(ValuePair event) {
                if (!event.isFromExternalSource()) {
                    return;
                }
                if (buffer != null) {
                    try {
                        buffer.close();
                    } catch (IOException e) {
                        LOGGER.warn("", e);
                    }
                    buffer = null;
                }
            }
        });
        bufferPurgeEnabledNode = node.getChild(Util.BUFFER_PURGE_ENABLED, true);
        if (bufferPurgeEnabledNode == null) {
            bufferPurgeEnabledNode = node.createChild(Util.BUFFER_PURGE_ENABLED, true)
                                         .setValueType(ValueType.BOOL).setValue(new Value(false))
                                         .build();
        }
        bufferPurgeEnabledNode.setWritable(Writable.WRITE);
        bufferMaxSizeNode = node.getChild(Util.BUFFER_MAX_SIZE, true);
        if (bufferMaxSizeNode == null) {
            bufferMaxSizeNode = node.createChild(Util.BUFFER_MAX_SIZE, true)
                                    .setValueType(ValueType.NUMBER).setValue(new Value(1074000000))
                                    .build();
        }
        bufferMaxSizeNode.setWritable(Writable.WRITE);
        batchSize = node.getChild(Util.BATCH_WRITE_MAX_RECS, true);
        if (batchSize == null) {
            batchSize = node.createChild(Util.BATCH_WRITE_MAX_RECS, true)
                            .setValueType(ValueType.NUMBER).setValue(new Value(100))
                            .build();
        }
        batchSize.setWritable(Writable.WRITE);
        batchInterval = node.getChild(Util.BATCH_WRITE_MIN_IVL, true);
        if (batchInterval == null) {
            batchInterval = node.createChild(Util.BATCH_WRITE_MIN_IVL, true)
                                .setValueType(ValueType.NUMBER).setValue(new Value(1000))
                                .build();
        }
        batchInterval.setWritable(Writable.WRITE);
        siteNameNode = node.getChild(Util.SITE_NAME, true);
        if (siteNameNode == null) {
            siteNameNode = node.createChild(Util.SITE_NAME, true).setValueType(ValueType.STRING)
                               .setValue(new Value("")).build();
        }
        attrDefsNode = node.createChild(Util.ATTR_DEFINITIONS, true).setValueType(ValueType.ARRAY)
                           .build();
        attrDefsNode.setSerializable(false);
        creationNode = node.createChild(Util.CREATION_DATETIME, true).setValueType(ValueType.STRING)
                           .build();
        creationNode.setSerializable(false);
        gsisNode = node.createChild(Util.GLOBAL_SECONDARY_INDICES, true)
                       .setValueType(ValueType.ARRAY).build();
        gsisNode.setSerializable(false);
        itemCountNode = node.createChild(Util.ITEM_COUNT, true).setValueType(ValueType.NUMBER)
                            .build();
        itemCountNode.setSerializable(false);
        keySchemaNode = node.createChild(Util.KEY_SCHEMA, true).setValueType(ValueType.ARRAY)
                            .build();
        keySchemaNode.setSerializable(false);
        streamArnNode = node.createChild(Util.STREAM_ARN, true).setValueType(ValueType.STRING)
                            .build();
        streamArnNode.setSerializable(false);
        streamLabelNode = node.createChild(Util.STREAM_LABEL, true).setValueType(ValueType.STRING)
                              .build();
        streamLabelNode.setSerializable(false);
        lsisNode = node.createChild(Util.LOCAL_SECONDARY_INDICES, true)
                       .setValueType(ValueType.ARRAY).build();
        lsisNode.setSerializable(false);
        provThroughputNode = node.createChild(Util.PROVISIONED_THROUGHPUT, true)
                                 .setValueType(ValueType.MAP).build();
        provThroughputNode.setSerializable(false);
        streamSpecNode = node.createChild(Util.STREAM_SPEC, true).setValueType(ValueType.STRING)
                             .build();
        streamSpecNode.setSerializable(false);
        tableArnNode = node.createChild(Util.TABLE_ARN, true).setValueType(ValueType.STRING)
                           .build();
        tableArnNode.setSerializable(false);
        tableNameNode = node.createChild(Util.TABLE_NAME, true).setValueType(ValueType.STRING)
                            .build();
        tableNameNode.setSerializable(false);
        tableSizeNode = node.createChild(Util.TABLE_SIZE_BYTES, true).setValueType(ValueType.NUMBER)
                            .build();
        tableSizeNode.setSerializable(false);
        tableStatusNode = node.createChild(Util.TABLE_STATUS, true).setValueType(ValueType.STRING)
                              .build();
        tableStatusNode.setSerializable(false);
        ttlStatusNode = node.createChild(Util.TTL_STATUS, true).setValueType(ValueType.STRING)
                            .build();
        ttlStatusNode.setSerializable(false);
        ttlEnabledNode = node.getChild(Util.TTL_ENABLED, true);
        if (ttlEnabledNode == null) {
            ttlEnabledNode = node.createChild(Util.TTL_ENABLED, true).setValueType(ValueType.BOOL)
                                 .setValue(new Value(false)).build();
        } else if (ttlEnabledNode.getValue() != null) {
            threadPool.schedule(new Runnable() {
                @Override
                public void run() {
                    Regions region = Util.getRegionFromNode(node);
                    setTTLEnabled(node.getName(), region, ttlEnabledNode.getValue().getBool());
                }
            }, 0, TimeUnit.MILLISECONDS);
        }
        ttlEnabledNode.getListener().setValueHandler(new Handler<ValuePair>() {
            @Override
            public void handle(ValuePair event) {
                if (event.isFromExternalSource()) {
                    Regions region = Util.getRegionFromNode(node);
                    setTTLEnabled(node.getName(), region, event.getCurrent().getBool());
                }
            }
        });
        ttlEnabledNode.setWritable(Writable.WRITE);
        ttlDefaultDaysNode = node.getChild(Util.TTL_DEFAULT, true);
        if (ttlDefaultDaysNode == null) {
            ttlDefaultDaysNode = node.createChild(Util.TTL_DEFAULT, true)
                                     .setValueType(ValueType.NUMBER).setValue(new Value(1461))
                                     .build();
        }
        ttlDefaultDaysNode.setWritable(Writable.WRITE);
        tableInfoPoller = threadPool.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                refreshTableDetails(node);
            }
        }, 0, 6, TimeUnit.HOURS);
        unsentInBuffer = true;
        initialized = true;
        write();
    }

    @Override
    public boolean isPurgeEnabled() {
        return bufferPurgeEnabledNode.getValue().getBool();
    }

    @Override
    public void query(String path, long from, long to, CompleteHandler<QueryData> handler) {
        Map<String, AttributeValue> eav = new HashMap<String, AttributeValue>();
        eav.put(":v1", new AttributeValue().withS(prependToPath(path)));
        eav.put(":v2", new AttributeValue().withN(String.valueOf(from)));
        eav.put(":v3", new AttributeValue().withN(String.valueOf(to)));

        DynamoDBQueryExpression<DBEntry> queryExpression = new DynamoDBQueryExpression<DBEntry>()
                .withKeyConditionExpression(
                        Util.WATCH_PATH_KEY + " = :v1 and " + Util.TS_KEY + " between :v2 and :v3")
                .withExpressionAttributeValues(eav);
        List<DBEntry> results = mapper.query(DBEntry.class, queryExpression);

        for (DBEntry result : results) {
            handler.handle(new QueryData(new Value(result.getValue()), result.getTs()));
        }
        handler.complete();
    }

    @Override
    public QueryData queryFirst(String path) {
        Map<String, AttributeValue> eav = new HashMap<String, AttributeValue>();
        eav.put(":v1", new AttributeValue().withS(prependToPath(path)));

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
        eav.put(":v1", new AttributeValue().withS(prependToPath(path)));

        DynamoDBQueryExpression<DBEntry> queryExpression = new DynamoDBQueryExpression<DBEntry>()
                .withKeyConditionExpression(Util.WATCH_PATH_KEY + " = :v1")
                .withExpressionAttributeValues(eav)
                .withLimit(1)
                .withScanIndexForward(false);
        List<DBEntry> results = mapper.query(DBEntry.class, queryExpression);

        DBEntry first = results.isEmpty() ? null : results.get(0);
        return first == null ? null : new QueryData(new Value(first.getValue()), first.getTs());
    }

    public void write(String path, Value value, long ts, long expiration) {
        Record r = new Record(path, ts, value, expiration);
        synchronized (writeQueue) {
            writeQueue.add(r);
        }
        write();
    }

    @Override
    public void write(String path, Value value, long ts) {
        write(path, value, ts, getExpiration());
    }

    @Override
    protected void performConnect() throws Exception {
    }

    protected void refreshTableDetails(final Node node) {
        Regions region = Util.getRegionFromNode(node);
        TableDescription tableInfo = provider.getTableInfo(node.getName(), region);

        List<AttributeDefinition> attrDefs = tableInfo.getAttributeDefinitions();
        if (attrDefs != null) {
            JsonArray ja = new JsonArray();
            for (AttributeDefinition ad : attrDefs) {
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
            for (GlobalSecondaryIndexDescription gsi : gsis) {
                ja.add(gsi.toString());
            }
            gsisNode.setValue(new Value(ja));
        }

        long itemCount = tableInfo.getItemCount();
        itemCountNode.setValue(new Value(itemCount));

        List<KeySchemaElement> keySchema = tableInfo.getKeySchema();
        if (keySchema != null) {
            JsonArray ja = new JsonArray();
            for (KeySchemaElement elem : keySchema) {
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
            for (LocalSecondaryIndexDescription lsi : lsis) {
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
            jo.put("Last Decrease Date",
                   lastDecrease != null ? TimeUtils.format(lastDecrease) : null);
            jo.put("Last Increase Date",
                   lastIncrease != null ? TimeUtils.format(lastIncrease) : null);
            jo.put("Number of Decreases Today", numDecreases);
            provThroughputNode.setValue(new Value(jo));

            Action act = new Action(Permission.READ, new Handler<ActionResult>() {
                @Override
                public void handle(ActionResult event) {
                    long rcu = event.getParameter(Util.RCU, ValueType.NUMBER).getNumber()
                                    .longValue();
                    long wcu = event.getParameter(Util.WCU, ValueType.NUMBER).getNumber()
                                    .longValue();
                    Regions region = Util.getRegionFromNode(node);
                    provider.updateTable(node.getName(), region, rcu, wcu);
                    refreshTableDetails(node);
                }
            });
            act.addParameter(new Parameter(Util.RCU, ValueType.NUMBER, new Value(latestRCU)));
            act.addParameter(new Parameter(Util.WCU, ValueType.NUMBER, new Value(latestWCU)));
            Node anode = node.getChild(Util.EDIT_TABLE, true);
            if (anode == null) {
                node.createChild(Util.EDIT_TABLE, true).setAction(act).build()
                    .setSerializable(false);
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

        refreshTTLStatus(tableName, region);
    }

    Node getNode() {
        return myNode;
    }

    boolean isInitialized() {
        return initialized;
    }

    void setTTLEnabled(String tableName, Regions region, boolean enabled) {
        provider.updateTTL(tableName, region, enabled);
        refreshTTLStatus(tableName, region);
    }

    private void checkIfQueueTooLarge() {
        //prevent out of memory, write queue to disk if getting too large.
        boolean needBufferWrite = false;
        //calculate number max number of records that can be written in 15 minutes
        long batchIvl = getBatchMinIvl();
        long maxQueue;
        if (batchIvl > 900000) { //ivl > 15 mins
            maxQueue = getBatchMaxSize();
        } else {
            maxQueue = (900000 / batchIvl) * getBatchMaxSize();
        }
        synchronized (writeQueue) {
            needBufferWrite = writeQueue.size() > maxQueue;
        }
        if (needBufferWrite) {
            LOGGER.warn("Queue larger than can be written in 15 minutes, flushing to disk");
            unsentInBuffer = true;
            writeToBuffer();
        }
    }

    private int getBatchMaxSize() {
        int min = batchSize.getValue().getNumber().intValue();
        return Math.max(min, 1);
    }

    private int getBatchMinIvl() {
        int min = batchInterval.getValue().getNumber().intValue();
        return Math.max(min, 1);
    }

    private void initBuffer() {
        String bufPath = bufferPathNode.getValue().getString();
        buffer = DatabaseFactory.createDatabase(new File(bufPath), new ValueSerializer());
        provider.getPurger().addDb(buffer, this);
    }

    private String prependToPath(String path) {
        if (path.isEmpty() || path.charAt(0) != '/') {
            path = "/" + path;
        }
        return siteNameNode.getValue().getString() + path;
    }

    private void refreshTTLStatus(String tableName, Regions region) {
        TimeToLiveDescription ttlDesc = provider.getTTLInfo(tableName, region);
        if (ttlDesc != null) {
            String ttlStatus = ttlDesc.getTimeToLiveStatus();
            if (ttlStatus != null) {
                ttlStatusNode.setValue(new Value(ttlStatus));
            }
        }
    }

    private void write() {
        synchronized (writeQueue) {
            if (writing) {
                return;
            }
            writing = true;
        }
        threadPool.execute(writeRunner);
    }

    private void writeFromBuffer() {
        if (buffer == null) {
            initBuffer();
        }
        List<String> series = Util.getSanitizedSeriesIds(buffer);
        if (series.isEmpty()) {
            unsentInBuffer = false;
            writeFromQueue();
            return;
        }
        int maxRecs = getBatchMaxSize();
        Map<String, Long> toDelete = new HashMap<>();
        final AtomicLong lastTs = new AtomicLong();
        final List<DBEntry> updates = new LinkedList<>();
        for (String path : series) {
            lastTs.set(0);
            buffer.query(path, 0, Long.MAX_VALUE, maxRecs - updates.size(),
                         new QueryCallback<ByteData>() {
                             @Override
                             public void sample(String seriesId, long ts, ByteData valueData) {
                                 DBEntry entry = new DBEntry();
                                 entry.setWatchPath(prependToPath(seriesId));
                                 entry.setTs(ts);
                                 entry.setValue(valueData.getValue().toString());
                                 entry.setExpiration(getExpiration(ts));
                                 updates.add(entry);
                                 if (ts > lastTs.get()) {
                                     lastTs.set(ts);
                                 }
                             }
                         });
            toDelete.put(path, lastTs.longValue());
        }
        if (updates.size() == 0) {
            for (Map.Entry<String, Long> e : toDelete.entrySet()) {
                buffer.deleteSeries(e.getKey());
            }
            unsentInBuffer = false;
            writeFromQueue();
            return;
        }
        unsentInBuffer = true;
        if (batchWrite(updates)) {
            for (Map.Entry<String, Long> e : toDelete.entrySet()) {
                long toTs = e.getValue();
                if (toTs == 0) {
                    buffer.deleteSeries(e.getKey());
                } else {
                    buffer.delete(e.getKey(), 0, toTs);
                }
            }
            checkIfQueueTooLarge();
        } else {
            writeRunner.setDelay(10000);
            writeToBuffer();
        }
    }

    private void writeFromQueue() {
        int maxSize = getBatchMaxSize();
        List<Record> tmp = new ArrayList<>(maxSize);
        synchronized (writeQueue) {
            int size = Math.min(maxSize, writeQueue.size());
            if (size == 0) {
                return;
            }
            for (int i = 0; i < size; i++) {
                tmp.add(writeQueue.get(i));
            }
        }
        int size = tmp.size();
        List<DBEntry> entries = new ArrayList<>(tmp.size());
        for (Record r : tmp) {
            entries.add(r.toEntry());
        }
        if (batchWrite(entries)) {
            synchronized (writeQueue) {
                for (int i = 0; i < size; i++) {
                    writeQueue.remove(0);
                }
            }
            checkIfQueueTooLarge();
        } else {
            writeRunner.setDelay(10000);
            unsentInBuffer = true;
        }
    }

    private void writeToBuffer() {
        List<Record> entries;
        synchronized (writeQueue) {
            if (writeQueue.isEmpty()) {
                return;
            }
            entries = new ArrayList<>();
            entries.addAll(writeQueue);
            writeQueue.clear();
        }
        LOGGER.info("Storing unsent updates in buffer");
        for (Record e : entries) {
            ByteData d = new ByteData();
            d.setValue(e.value);
            buffer.write(e.path, e.ts, d);
        }
    }

    private class Record {

        long expiration;
        String path;
        long ts;
        Value value;

        Record(String path, long ts, Value value, long expiration) {
            this.path = path;
            this.ts = ts;
            this.value = value;
            this.expiration = expiration;
        }

        DBEntry toEntry() {
            DBEntry entry = new DBEntry();
            entry.setWatchPath(prependToPath(path));
            entry.setTs(ts);
            entry.setValue(value.toString());
            entry.setExpiration(expiration);
            return entry;
        }
    }

    private class WriteRunner implements Runnable {

        long delay;
        long last = System.currentTimeMillis();

        public void run() {
            delay = getBatchMinIvl();
            long next = last + delay;
            long time = System.currentTimeMillis();
            if (next > time) {
                try {
                    Thread.sleep(next - time);
                } catch (Exception ignore) {
                }
            }
            try {
                if (unsentInBuffer) {
                    writeFromBuffer();
                } else {
                    writeFromQueue();
                }
            } catch (Exception x) {
                LOGGER.error("", x);
            }
            last = System.currentTimeMillis();
            boolean more = unsentInBuffer;
            synchronized (writeQueue) {
                if (!writeQueue.isEmpty()) {
                    more = true;
                }
                writing = false;
            }
            if (more) {
                write();
            }
        }

        void setDelay(long delay) {
            this.delay = delay;
        }

    }

}
