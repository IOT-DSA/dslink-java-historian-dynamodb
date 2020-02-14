package org.iot.dsa.dynamodb.db;

public interface PurgeSettings {

    long getMaxSizeInBytes();

    boolean isPurgeEnabled();

}
