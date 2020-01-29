package org.iot.dsa.dynamodb.db;

public interface PurgeSettings {

    public long getMaxSizeInBytes();

    public boolean isPurgeEnabled();

}
