package org.iot.dsa.dynamodb.db;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTimeToLiveRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTimeToLiveResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TimeToLiveDescription;
import com.amazonaws.services.dynamodbv2.model.TimeToLiveSpecification;
import com.amazonaws.services.dynamodbv2.model.UpdateTimeToLiveRequest;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.actions.ActionResult;
import org.dsa.iot.dslink.node.actions.EditorType;
import org.dsa.iot.dslink.node.actions.Parameter;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.dslink.util.Objects;
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

public class DynamoDBProvider extends DatabaseProvider {

    private static final String SCHEMA = "schema";
    private static final String TABLE_SUFFIX_PATHS = "_DSALinkPaths";
    private static final String TABLE_SUFFIX_SITES = "_DSALinkSites";

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBProvider.class);
    private final BufferPurger purger = new BufferPurger();
    private Map<Regions, DynamoDBRegion> regionMap = new HashMap<Regions, DynamoDBRegion>();

    public DynamoDBProvider() {
        purger.setupPurger();
    }

    @Override
    public Action createDbAction(Permission perm) {
        Action act = new Action(perm, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                String historyName = event.getParameter(Util.EXISTING_TABLE_NAME).getString();
                Regions region = Regions.fromName(event.getParameter(Util.REGION).getString());
                String site = event.getParameter(Util.SITE_NAME, ValueType.STRING).getString();
                if (Util.NEW_TABLE_OPTION.equals(historyName)) {
                    historyName = event.getParameter(Util.NEW_TABLE_NAME, ValueType.STRING)
                                       .getString();
                    long rcu = event.getParameter(Util.NEW_TABLE_RCU, ValueType.NUMBER).getNumber()
                                    .longValue();
                    long wcu = event.getParameter(Util.NEW_TABLE_WCU, ValueType.NUMBER).getNumber()
                                    .longValue();
                    try {
                        if ((site != null) && !site.isEmpty()) {
                            createSitesTable(historyName, region, rcu, wcu);
                            addSite(historyName, site, region);
                            createPathsTable(historyName, region, rcu, wcu);
                        }
                        createHistoryTable(historyName, region, rcu, wcu);
                    } catch (Exception e) {
                        LOGGER.error("CreateTable request failed for " + historyName);
                        LOGGER.error(e.getMessage());
                        historyName = null;
                    }
                } else if (Util.OTHER_TABLE_OPTION.equals(historyName)) {
                    historyName = event.getParameter(Util.NEW_TABLE_NAME, ValueType.STRING)
                                       .getString();
                }
                if (historyName != null) {
                    NodeBuilder builder = createDbNode(historyName, event);
                    builder.setRoConfig(Util.REGION, new Value(region.getName()));
                    Node table = builder.build();
                    createAndInitDb(table);
                    table.getChild(Util.SITE_NAME, true).setValue(new Value(site));
                    table.setRoConfig(SCHEMA, new Value(new Date().toString()));
                }
            }
        });
        List<String> dropdown = new LinkedList<String>();
        dropdown.add(Util.NEW_TABLE_OPTION);
        dropdown.add(Util.OTHER_TABLE_OPTION);

        try {
            TableCollection<ListTablesResult> tables = getDynamoDB().listTables();
            Iterator<Table> iterator = tables.iterator();
            String name;
            while (iterator.hasNext()) {
                Table table = iterator.next();
                name = table.getTableName();
                if (!name.endsWith(TABLE_SUFFIX_PATHS) && !name.endsWith(TABLE_SUFFIX_SITES)) {
                    dropdown.add(name);
                }
            }
        } catch (Exception e) {
            LOGGER.warn("", e);
        }
        act.addParameter(new Parameter(Util.SITE_NAME, ValueType.STRING)
                                 .setDescription("New sites must have unique names")
                                 .setPlaceHolder("Cannot ever be changed"));
        act.addParameter(new Parameter(Util.EXISTING_TABLE_NAME, ValueType.makeEnum(dropdown),
                                       new Value(Util.NEW_TABLE_OPTION)));
        act.addParameter(new Parameter(Util.REGION, ValueType.makeEnum(Util.getRegionList()),
                                       new Value(Main.getInstance().getDefaultRegion().getName())));
        act.addParameter(new Parameter(Util.NEW_TABLE_NAME, ValueType.STRING).setDescription(
                "Only applicable when choosing 'Create new table' or 'Other table'"));
        act.addParameter(new Parameter(Util.NEW_TABLE_RCU, ValueType.NUMBER, new Value(5L))
                                 .setDescription("Only applicable when creating new table"));
        act.addParameter(new Parameter(Util.NEW_TABLE_WCU, ValueType.NUMBER, new Value(6L))
                                 .setDescription("Only applicable when creating new table"));
        return act;
    }

    @Override
    public Permission dbPermission() {
        return Permission.CONFIG;
    }

    @Override
    public void deleteRange(Watch watch, long fromTs, long toTs) {
        DynamoDBProxy db = (DynamoDBProxy) watch.getGroup().getDb();
        db.delete(watch.getPath(), fromTs, toTs);
    }

    public BufferPurger getPurger() {
        return purger;
    }

    @Override
    public void onWatchAdded(final Watch watch) {
        final Node node = watch.getNode();
        final Database database = watch.getGroup().getDb();
        final Permission perm = database.getProvider().dbPermission();

        //Add watch path the the path table
        final DynamoDBProxy db = (DynamoDBProxy) watch.getGroup().getDb();
        if (db.isInitialized()) {
            final String site = db.getSiteName();
            if ((site != null) && !site.isEmpty()) {
                //Only add new watches, use a config to track when created
                Value v = node.getRoConfig(SCHEMA);
                if ((v == null) && (db.getNode().getRoConfig(SCHEMA) != null)) {
                    Objects.getDaemonThreadPool().schedule(new Runnable() {
                        @Override
                        public void run() {
                            addPath(db.getTableName(), site, watch.getPath(),
                                    Util.getRegionFromNode(node));
                            node.setRoConfig(SCHEMA, new Value(new Date().toString()));
                        }
                    }, 2, TimeUnit.SECONDS);
                }
            }
        }

        NodeBuilder b = node.createChild("unsubPurge", true)
                            .setDisplayName("Unsubscribe and Purge");
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
        a.addParameter(
                new Parameter("Timerange", ValueType.STRING).setEditorType(EditorType.DATE_RANGE)
                                                            .setDescription(
                                                                    "The range for which to purge data"));
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
        //DynamoDBProxy db = (DynamoDBProxy) watch.getGroup().getDb();
        a.addParameter(new Parameter(Util.TS, ValueType.STRING));
        a.addParameter(new Parameter(Util.VALUE, ValueType.STRING));
        a.addParameter(new Parameter(Util.TTL, ValueType.STRING,
                                     new Value(TimeUtils.format(db.getExpiration()))));
        b.setAction(a);
        b.build();

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

    public void stop() {
        purger.stop();
    }

    @Override
    protected Database createDb(Node node) {
        String tableName = node.getName();
        DynamoDBMapperConfig mapperConfig = DynamoDBMapperConfig.builder().withTableNameOverride(
                TableNameOverride.withTableNameReplacement(tableName)).build();
        Regions region = Util.getRegionFromNode(node);
        DynamoDBMapper mapper = new DynamoDBMapper(getClient(region), mapperConfig);
        return new DynamoDBProxy(node, this, mapper);
    }

    TimeToLiveDescription getTTLInfo(String tableName, Regions region) {
        DescribeTimeToLiveRequest getTTLReq = new DescribeTimeToLiveRequest()
                .withTableName(tableName);
        DescribeTimeToLiveResult result = getClient(region).describeTimeToLive(getTTLReq);
        return result.getTimeToLiveDescription();
    }

    TableDescription getTableInfo(String tableName, Regions region) {
        Table table = getDynamoDB(region).getTable(tableName);
        return table.describe();
    }

    void updateTTL(String tableName, Regions region, boolean enabled) {
        TimeToLiveSpecification ttlSpec = new TimeToLiveSpecification()
                .withAttributeName(Util.TTL)
                .withEnabled(enabled);
        UpdateTimeToLiveRequest ttlReq = new UpdateTimeToLiveRequest()
                .withTableName(tableName)
                .withTimeToLiveSpecification(ttlSpec);
        try {
            getClient(region).updateTimeToLive(ttlReq);
        } catch (AmazonDynamoDBException e) {
            LOGGER.debug("", e);
        }
    }

    void updateTable(String tableName, Regions region, long rcu, long wcu) {
        try {
            Table table = getDynamoDB(region).getTable(tableName);
            table.updateTable(new ProvisionedThroughput().withReadCapacityUnits(rcu)
                                                         .withWriteCapacityUnits(wcu));
            table.waitForActive();
        } catch (Exception e) {
            LOGGER.error("UpdateTable request failed for " + tableName);
            LOGGER.error(e.getMessage());
        }
    }

    private void addPath(String historyName, String site, String path, Regions region) {
        Item res = null;
        DynamoDB db = getDynamoDB(region);
        Table table = db.getTable(historyName + TABLE_SUFFIX_PATHS);
        if (res == null) {
            table.putItem(
                    new Item().withPrimaryKey(Util.SITE_KEY, site, Util.WATCH_PATH_KEY, path));
        }
    }

    private void addSite(String historyName, String site, Regions region) {
        Item res = null;
        DynamoDB db = getDynamoDB(region);
        Table table = db.getTable(historyName + TABLE_SUFFIX_SITES);
        try {
            GetItemSpec req = new GetItemSpec().withPrimaryKey(Util.SITE_KEY, site);
            res = table.getItem(req);
        } catch (Exception x) {
            LOGGER.warn("Check to see if site table has site", x);
        }
        if (res == null) {
            TimeZone tz = TimeZone.getDefault();
            table.putItem(new Item().withPrimaryKey(Util.SITE_KEY, site)
                                    .with(Util.TIMEZONE, tz.getID()));
        }
    }

    private void createHistoryTable(String tableName, Regions region, long rcu, long wcu)
            throws Exception {
        List<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
        attributeDefinitions.add(new AttributeDefinition().withAttributeName(Util.WATCH_PATH_KEY)
                                                          .withAttributeType("S"));
        attributeDefinitions.add(new AttributeDefinition().withAttributeName(Util.TS_KEY)
                                                          .withAttributeType("N"));
//            attributeDefinitions.add(new AttributeDefinition().withAttributeName(Util.VALUE).withAttributeType("S"));

        List<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
        keySchema.add(new KeySchemaElement().withAttributeName(Util.WATCH_PATH_KEY)
                                            .withKeyType(KeyType.HASH));
        keySchema.add(new KeySchemaElement().withAttributeName(Util.TS_KEY)
                                            .withKeyType(KeyType.RANGE));

        CreateTableRequest request = new CreateTableRequest()
                .withTableName(tableName)
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withProvisionedThroughput(new ProvisionedThroughput()
                                                   .withReadCapacityUnits(rcu)
                                                   .withWriteCapacityUnits(wcu));
        Table table = getDynamoDB(region).createTable(request);
        table.waitForActive();
    }

    private void createPathsTable(String tableName, Regions region, long rcu, long wcu) {
        try {
            List<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
            attributeDefinitions.add(
                    new AttributeDefinition().withAttributeName(Util.SITE_KEY)
                                             .withAttributeType("S"));
            attributeDefinitions.add(
                    new AttributeDefinition().withAttributeName(Util.WATCH_PATH_KEY)
                                             .withAttributeType("S"));
            List<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
            keySchema.add(new KeySchemaElement().withAttributeName(Util.SITE_KEY)
                                                .withKeyType(KeyType.HASH));
            keySchema.add(new KeySchemaElement().withAttributeName(Util.WATCH_PATH_KEY)
                                                .withKeyType(KeyType.RANGE));
            CreateTableRequest request = new CreateTableRequest()
                    .withTableName(tableName + TABLE_SUFFIX_PATHS)
                    .withKeySchema(keySchema)
                    .withAttributeDefinitions(attributeDefinitions)
                    .withProvisionedThroughput(new ProvisionedThroughput()
                                                       .withReadCapacityUnits(rcu)
                                                       .withWriteCapacityUnits(wcu));
            Table table = getDynamoDB(region).createTable(request);
            try {
                table.waitForActive();
            } catch (InterruptedException whocares) {
            }
        } catch (ResourceInUseException expected) {
        }
    }

    private void createSitesTable(String historyName, Regions region, long rcu, long wcu) {
        try {
            List<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
            attributeDefinitions.add(new AttributeDefinition().withAttributeName(Util.SITE_KEY)
                                                              .withAttributeType("S"));
            List<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
            keySchema.add(new KeySchemaElement().withAttributeName(Util.SITE_KEY)
                                                .withKeyType(KeyType.HASH));
            CreateTableRequest request = new CreateTableRequest()
                    .withTableName(historyName + TABLE_SUFFIX_SITES)
                    .withKeySchema(keySchema)
                    .withAttributeDefinitions(attributeDefinitions)
                    .withProvisionedThroughput(new ProvisionedThroughput()
                                                       .withReadCapacityUnits(rcu)
                                                       .withWriteCapacityUnits(wcu));
            Table table = getDynamoDB(region).createTable(request);
            try {
                table.waitForActive();
            } catch (InterruptedException whocares) {
            }
        } catch (ResourceInUseException expected) {
        }
    }

    private AmazonDynamoDB getClient(Regions region) {
        return getRegionObject(region).getClient();
    }

    private DynamoDB getDynamoDB() {
        return getDynamoDB(Main.getInstance().getDefaultRegion());
    }

    private DynamoDB getDynamoDB(Regions region) {
        return getRegionObject(region).getDynamoDB();
    }

    private DynamoDBRegion getRegionObject(Regions region) {
        DynamoDBRegion regionObj = regionMap.get(region);
        if (regionObj == null) {
            regionObj = new DynamoDBRegion(region);
            regionMap.put(region, regionObj);
        }
        return regionObj;
    }
}
