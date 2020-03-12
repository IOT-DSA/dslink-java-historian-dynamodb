package org.iot.dsa.dynamodb.db;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

@DynamoDBTable(tableName = "DB")
public class DBEntry {

    private Long expiration;
    private Long ts;
    private String value;
    private String watchPath;

    @DynamoDBAttribute
    public Long getExpiration() {
        return expiration;
    }

    public void setExpiration(Long expiration) {
        this.expiration = expiration;
    }

    @DynamoDBRangeKey
    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    @DynamoDBAttribute
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @DynamoDBHashKey
    public String getWatchPath() {
        return watchPath;
    }

    public void setWatchPath(String watchPath) {
        this.watchPath = watchPath;
    }


}
