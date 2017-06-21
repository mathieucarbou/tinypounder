package org.terracotta.tinypounder;

public class CacheConfiguration {
  private final long onHeapSize;
  private final String onHeapSizeUnit;
  private final long offHeapSize;
  private final String offHeapSizeUnit;
  private final long diskSize;
  private final String diskSizeUnit;
  private final long clusteredDedicatedSize;
  private final String clusteredDedicatedUnit;
  private final String clusteredSharedPoolName;
  private final ClusterTierType clusteredTierType;

  public CacheConfiguration(long onHeapSize, String onHeapSizeUnit, long offHeapSize, String offHeapSizeUnit, long diskSize, String diskSizeUnit, long clusteredDedicatedSize, String clusteredDedicatedUnit, String clusteredSharedPoolName, ClusterTierType clusteredTierType) {
    this.onHeapSize = onHeapSize;
    this.onHeapSizeUnit = onHeapSizeUnit;
    this.offHeapSize = offHeapSize;
    this.offHeapSizeUnit = offHeapSizeUnit;
    this.diskSize = diskSize;
    this.diskSizeUnit = diskSizeUnit;
    this.clusteredDedicatedSize = clusteredDedicatedSize;
    this.clusteredDedicatedUnit = clusteredDedicatedUnit;
    this.clusteredSharedPoolName = clusteredSharedPoolName;
    this.clusteredTierType = clusteredTierType;
  }

  public long getOnHeapSize() {
    return onHeapSize;
  }

  public String getOnHeapSizeUnit() {
    return onHeapSizeUnit;
  }

  public long getOffHeapSize() {
    return offHeapSize;
  }

  public String getOffHeapSizeUnit() {
    return offHeapSizeUnit;
  }

  public long getDiskSize() {
    return diskSize;
  }

  public String getDiskSizeUnit() {
    return diskSizeUnit;
  }

  public long getClusteredDedicatedSize() {
    return clusteredDedicatedSize;
  }

  public String getClusteredDedicatedUnit() {
    return clusteredDedicatedUnit;
  }

  public String getClusteredSharedPoolName() {
    return clusteredSharedPoolName;
  }

  public ClusterTierType getClusteredTierType() {
    return clusteredTierType;
  }


  public enum ClusterTierType {
    NONE,
    DEDICATED,
    SHARED
  }
}
