package org.iot.dsa.dynamodb;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.util.TimeUtils;
import org.dsa.iot.dslink.util.json.JsonArray;
import org.dsa.iot.dslink.util.json.JsonObject;
import org.iot.dsa.dynamodb.db.DBEntry;

import com.amazonaws.regions.Regions;

public class Util {
	
	public static final String CREDENTIALS = "Credentials";
	public static final String ACCESS_ID = "Access Key ID";
	public static final String ACCESS_SECRET = "Secret Access Key";
	public static final String REGION = "Region";
	public static final String NEW_TABLE_OPTION = "Create new table";
	public static final String OTHER_TABLE_OPTION = "Other table";
	public static final String EXISTING_TABLE_NAME = "Table";
	public static final String NEW_TABLE_NAME = "Name";
	public static final String NEW_TABLE_RCU = "Read Capacity Units";
	public static final String NEW_TABLE_WCU = "Write Capacity Units";
	public static final String RCU = "Read Capacity Units";
	public static final String WCU = "Write Capacity Units";
	
	public static final String ACT_SET_CREDENTIALS = "Set Credentials";
	public static final String ACT_SET_REGION = "Set Default Region";
	public static final String EDIT_TABLE = "Edit";
	
	public static final String WATCH_PATH_KEY = "watchPath";
	public static final String TS_KEY = "ts";
	public static final String VALUE = "value";
	public static final String TTL = "expiration";
	public static final String TS = "timestamp";
	
	public static final String PREFIX_ENABLED = "Prefix Enabled";
	public static final String PREFIX_VALUE = "Prefix";
	public static final String ATTR_DEFINITIONS = "Attribute Definitions";
	public static final String CREATION_DATETIME = "Creation Date";
	public static final String GLOBAL_SECONDARY_INDICES = "Global Secondary Indices";
	public static final String ITEM_COUNT = "Item Count";
	public static final String KEY_SCHEMA = "Key Schema";
	public static final String STREAM_ARN = "Latest Stream ARN";
	public static final String STREAM_LABEL = "Latest Stream Label";
	public static final String LOCAL_SECONDARY_INDICES = "Local Secondary Indices";
	public static final String PROVISIONED_THROUGHPUT = "Provisioned Throughput";
	public static final String STREAM_SPEC = "Stream Specification";
	public static final String TABLE_ARN = "Table ARN";
	public static final String TABLE_NAME = "Table Name";
	public static final String TABLE_SIZE_BYTES = "Table Size (Bytes)";
	public static final String TABLE_STATUS = "Table Status";
	public static final String TTL_ENABLED = "TTL Enabled";
	public static final String TTL_DEFAULT = "Default TTL for New Records (Days)";
	public static final String TTL_STATUS = "TTL Status";
	public static final String BUFFER_PATH = "Buffer Path";
	public static final String BUFFER_PURGE_ENABLED = "Enable Buffer Auto-Purge";
	public static final String BUFFER_MAX_SIZE = "Maximum Buffer Size in Bytes";
	
	public static DBEntry parseRecord(Object o) {
		if (o instanceof JsonObject) {
			return parseRecord((JsonObject) o);
		} else if (o instanceof JsonArray) {
			return parseRecord((JsonArray) o);
		} else if (o instanceof String) {
			String s = (String) o;
			if (s.charAt(0) != '[') {
				s = "[" + s;
			}
			if (s.charAt(s.length() - 1) != ']') {
				s = s + "]";
			}
			return parseRecord(new JsonArray(s));
		}
		return null;
	}
	
	public static DBEntry parseRecord(JsonObject jo) {
		Object tsO = jo.get(TS);
		Object valO = jo.get(VALUE);
		Object expO = jo.get(TTL);
		long ts = parseTs(tsO);
		String val = valO.toString();
		long exp = parseTs(expO);
		if (ts == -1 || val == null) {
			return null;
		}
		DBEntry entry = new DBEntry();
		entry.setTs(ts);
		entry.setValue(val);
		if (exp != -1) {
			entry.setExpiration(exp);
		}
		return entry;
	}
	
	public static DBEntry parseRecord(JsonArray ja) {
		if (ja.size() < 2) {
			return null;
		}
		long ts = parseTs(ja.get(0));
		int startInd = 0;
		if (ts == -1) {
			startInd = 1;
			ts = parseTs(ja.get(1));
		}
		if (ts == -1) {
			return null;
		}
		if (startInd + 1 >= ja.size()) {
			return null;
		}
		String val = ja.get(startInd + 1).toString();
		long exp = startInd + 2 < ja.size() ? parseTs(ja.get(startInd + 2)) : -1;
		
		DBEntry entry = new DBEntry();
		entry.setTs(ts);
		entry.setValue(val);
		if (exp != -1) {
			entry.setExpiration(exp);
		}
		return entry;
	}
	
	public static long parseTs(Object o) {
		if (o instanceof String ) {
			try {
				return TimeUtils.decode(o.toString());
			} catch (Exception e) {
			}
		}
		return -1;
	}
	
	public static String[] getRegionList() {
		Regions[] regions = Regions.values();
		String[] regionStrings = new String[regions.length];
		for (int i = 0; i < regions.length; i++) {
			Regions r = regions[i];
			regionStrings[i] = r.getName();
		}
		return regionStrings;
	}
	
	public static Regions getRegionFromNode(Node node) {
		Value rVal = node.getRoConfig(REGION);
		if (rVal == null) {
			node.setRoConfig(REGION, new Value(Regions.US_WEST_1.getName()));
			return Regions.US_WEST_1;
		}
		return Regions.fromName(rVal.getString());
	}
}
