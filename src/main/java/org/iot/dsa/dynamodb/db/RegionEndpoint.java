package org.iot.dsa.dynamodb.db;

import java.util.Objects;

import com.amazonaws.regions.Regions;

public class RegionEndpoint {
    
    private Regions region;
    private String endpoint;
    
    public RegionEndpoint(Regions region, String endpoint) {
        this.region = region;
        this.endpoint = endpoint;
    }
    
    public Regions getRegion() {
        return region;
    }
    
    public String getEndpoint() {
        return endpoint;
    }
    
    private boolean compareRegion(Regions other) {
        if (region == null) {
            return other == null;
        }
        return region.equals(other);
    }
    
    private boolean compareEndpoint(String other) {
        if (endpoint == null) {
            return other == null;
        }
        return endpoint.equals(other);
    }
    
    @Override
    public boolean equals(Object other) {
        if (other instanceof RegionEndpoint) {
            return compareRegion(((RegionEndpoint) other).getRegion()) && 
                    compareEndpoint(((RegionEndpoint) other).getEndpoint());
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(region, endpoint);
    }

}
