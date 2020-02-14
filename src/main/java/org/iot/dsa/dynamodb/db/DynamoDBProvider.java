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
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TimeToLiveDescription;
import com.amazonaws.services.dynamodbv2.model.TimeToLiveSpecification;
import com.amazonaws.services.dynamodbv2.model.UpdateTimeToLiveRequest;
import java.util.ArrayList;
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

    static final String SCHEMA = "schema";
    static final int SCHEMA_VERSION = 2;
    private static final String TABLE_SUFFIX_PATHS = "_DSALinkPaths";
    private static final String TABLE_SUFFIX_SITES = "_DSALinkSites";

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBProvider.class);
    private final BufferPurger purger = new BufferPurger();
    private final Map<Regions, DynamoDBRegion> regionMap = new HashMap<>();

    public DynamoDBProvider() {
        purger.setupPurger();
    }

    @Override
    public Action createDbAction(Permission perm) {
        Action act = new Action(perm, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                String site = event.getParameter(Util.SITE_NAME, ValueType.STRING).getString();
                if ((site == null) || site.isEmpty()) {
                    throw new IllegalArgumentException("Site Name is Required");
                }
                String historyName = event.getParameter(Util.EXISTING_TABLE_NAME).getString();
                Regions region = Regions.fromName(event.getParameter(Util.REGION).getString());
                if (Util.NEW_TABLE_OPTION.equals(historyName)) {
                    historyName = event.getParameter(Util.NEW_TABLE_NAME, ValueType.STRING)
                                       .getString();
                    if ((historyName == null) || historyName.isEmpty()) {
                        throw new IllegalArgumentException("Table Name is Required");
                    }
                    try {
                        createSitesTable(historyName, region);
                        addSite(historyName, site, region);
                        createPathsTable(historyName, region);
                        createHistoryTable(historyName, region);
                    } catch (Exception e) {
                        LOGGER.error("CreateTable request failed for " + historyName);
                        LOGGER.error(e.getMessage());
                        historyName = null;
                    }
                } else if (Util.OTHER_TABLE_OPTION.equals(historyName)) {
                    historyName = event.getParameter(Util.NEW_TABLE_NAME, ValueType.STRING)
                                       .getString();
                    if ((historyName == null) || historyName.isEmpty()) {
                        throw new IllegalArgumentException("Table Name is Required");
                    }
                }
                if (historyName != null) {
                    NodeBuilder builder = createDbNode(historyName, event);
                    builder.setRoConfig(Util.REGION, new Value(region.getName()));
                    Node table = builder.build();
                    table.setRoConfig(SCHEMA, new Value(SCHEMA_VERSION));
                    createAndInitDb(table);
                    table.getChild(Util.PREFIX, true).setValue(new Value(site));
                }
            }
        });
        List<String> dropdown = new LinkedList<>();
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
                                 .setPlaceHolder("Required and Uneditable"));
        act.addParameter(new Parameter(Util.EXISTING_TABLE_NAME, ValueType.makeEnum(dropdown),
                                       new Value(Util.NEW_TABLE_OPTION)));
        act.addParameter(new Parameter(Util.REGION, ValueType.makeEnum(Util.getRegionList()),
                                       new Value(Main.getInstance().getDefaultRegion().getName())));
        act.addParameter(new Parameter(Util.NEW_TABLE_NAME, ValueType.STRING)
                                 .setDescription(
                                         "Only applicable when choosing 'Create new table' or 'Other table'")
                                 .setPlaceHolder(
                                         "For 'Create new table' or 'Other table'"));
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
        final Node watchNode = watch.getNode();
        final Database database = watch.getGroup().getDb();
        final Permission perm = database.getProvider().dbPermission();
        final DynamoDBProxy db = (DynamoDBProxy) watch.getGroup().getDb();

        //Add watch path the the path table
        final String site = db.getSiteName();
        if ((site != null) && !site.isEmpty()) {
            int ver = getSchemaVersion(watchNode);
            if (ver == 1) {
                //development phase of schema 2
                watchNode.setRoConfig(SCHEMA, new Value(SCHEMA_VERSION));
            } else if (ver == 0) {
                //orig solo table version
                watchNode.setRoConfig(SCHEMA, new Value(SCHEMA_VERSION));
                Objects.getDaemonThreadPool().schedule(new Runnable() {
                    @Override
                    public void run() {
                        addPath(db.getNode().getName(), site, watch.getPath(),
                                Util.getRegionFromNode(watchNode));
                    }
                }, 0, TimeUnit.MILLISECONDS);
            }
        }

        NodeBuilder b = watchNode.createChild("unsubPurge", true)
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

        b = watchNode.createChild("purge", true).setDisplayName("Purge");
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

        b = watchNode.createChild("insert", true).setDisplayName("Insert Record");
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
        //upgrade old schemas
        int ver = getSchemaVersion(node);
        if (ver == 1) {
            //development phase of ver 2
            node.setRoConfig(SCHEMA, new Value(SCHEMA_VERSION));
        } else if (ver == 0) {
            node.setRoConfig(SCHEMA, new Value(SCHEMA_VERSION));
            //convert old prefix to site name
            String siteName = null;
            Node prefixEnabled = node.getChild(Util.PREFIX_ENABLED, true);
            if (prefixEnabled != null) {
                Node prefix = node.getChild(Util.PREFIX, true);
                if (prefixEnabled.getValue().getBool()) {
                    if (prefix != null) {
                        siteName = prefix.getValue().toString();
                    }
                } else {
                    if (prefix != null) {
                        prefix.setValue(new Value(""));
                    }
                }
                if (prefix != null) {
                    prefix.setDisplayName(Util.SITE_NAME);
                }
                prefixEnabled.delete(true);
            }
            //create / populate the sites / paths tables
            if ((siteName != null) && !siteName.isEmpty()) {
                createSitesTable(tableName, region);
                addSite(tableName, siteName, region);
                createPathsTable(tableName, region);
            }
        }
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

    private void addPath(String historyName, String site, String path, Regions region) {
        DynamoDB db = getDynamoDB(region);
        Table table = db.getTable(historyName + TABLE_SUFFIX_PATHS);
        table.putItem(
                new Item().withPrimaryKey(Util.SITE_KEY, site, Util.WATCH_PATH_KEY, path));
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

    private void createHistoryTable(String historyName, Regions region)
            throws Exception {
        try {
            Table table = getDynamoDB(region).getTable(historyName);
            TableDescription desc = table.describe();
            if (desc != null) {
                return;
            }
        } catch (ResourceNotFoundException expected) {
        }
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition().withAttributeName(Util.WATCH_PATH_KEY)
                                                          .withAttributeType("S"));
        attributeDefinitions.add(new AttributeDefinition().withAttributeName(Util.TS_KEY)
                                                          .withAttributeType("N"));

        List<KeySchemaElement> keySchema = new ArrayList<>();
        keySchema.add(new KeySchemaElement().withAttributeName(Util.WATCH_PATH_KEY)
                                            .withKeyType(KeyType.HASH));
        keySchema.add(new KeySchemaElement().withAttributeName(Util.TS_KEY)
                                            .withKeyType(KeyType.RANGE));

        CreateTableRequest request = new CreateTableRequest()
                .withTableName(historyName)
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(5L)
                                                                      .withWriteCapacityUnits(5L));
        Table table = getDynamoDB(region).createTable(request);
        table.waitForActive();
    }

    private void createPathsTable(String historyName, Regions region) {
        try {
            Table table = getDynamoDB(region).getTable(historyName + TABLE_SUFFIX_PATHS);
            TableDescription desc = table.describe();
            if (desc != null) {
                return;
            }
        } catch (ResourceNotFoundException expected) {
        }
        try {
            List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
            attributeDefinitions.add(
                    new AttributeDefinition().withAttributeName(Util.SITE_KEY)
                                             .withAttributeType("S"));
            attributeDefinitions.add(
                    new AttributeDefinition().withAttributeName(Util.WATCH_PATH_KEY)
                                             .withAttributeType("S"));
            List<KeySchemaElement> keySchema = new ArrayList<>();
            keySchema.add(new KeySchemaElement().withAttributeName(Util.SITE_KEY)
                                                .withKeyType(KeyType.HASH));
            keySchema.add(new KeySchemaElement().withAttributeName(Util.WATCH_PATH_KEY)
                                                .withKeyType(KeyType.RANGE));
            CreateTableRequest request = new CreateTableRequest()
                    .withTableName(historyName + TABLE_SUFFIX_PATHS)
                    .withKeySchema(keySchema)
                    .withAttributeDefinitions(attributeDefinitions)
                    .withProvisionedThroughput(new ProvisionedThroughput()
                                                       .withReadCapacityUnits(5L)
                                                       .withWriteCapacityUnits(5L));
            Table table = getDynamoDB(region).createTable(request);
            try {
                table.waitForActive();
            } catch (InterruptedException whocares) {
            }
        } catch (ResourceInUseException expected) {
        }
    }

    private void createSitesTable(String historyName, Regions region) {
        try {
            Table table = getDynamoDB(region).getTable(historyName + TABLE_SUFFIX_SITES);
            TableDescription desc = table.describe();
            if (desc != null) {
                return;
            }
        } catch (ResourceNotFoundException expected) {
        }
        try {
            List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
            attributeDefinitions.add(new AttributeDefinition().withAttributeName(Util.SITE_KEY)
                                                              .withAttributeType("S"));
            List<KeySchemaElement> keySchema = new ArrayList<>();
            keySchema.add(new KeySchemaElement().withAttributeName(Util.SITE_KEY)
                                                .withKeyType(KeyType.HASH));
            CreateTableRequest request = new CreateTableRequest()
                    .withTableName(historyName + TABLE_SUFFIX_SITES)
                    .withKeySchema(keySchema)
                    .withAttributeDefinitions(attributeDefinitions)
                    .withProvisionedThroughput(new ProvisionedThroughput()
                                                       .withReadCapacityUnits(5L)
                                                       .withWriteCapacityUnits(5L));
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

    private int getSchemaVersion(Node node) {
        Value v = node.getRoConfig(SCHEMA);
        if (v == null) {
            return 0;
        }
        if (v.getType() != ValueType.NUMBER) {
            return 1; //originally was a date string
        }
        return v.getNumber().intValue();
    }

}
