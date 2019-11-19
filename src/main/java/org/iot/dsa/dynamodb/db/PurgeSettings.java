package org.iot.dsa.dynamodb.db;

public interface PurgeSettings {
    
    public boolean isPurgeEnabled();
    
    public long getMaxSizeInBytes();
    
}
