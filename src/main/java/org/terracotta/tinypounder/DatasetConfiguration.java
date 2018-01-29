package org.terracotta.tinypounder;

public class DatasetConfiguration {
  private final String keyType;
  private final String offheapResourceName;
  private final String diskResourceName;
  private final boolean useIndex;

  public DatasetConfiguration(String keyType, String offheapResourceName, String diskResourceName, boolean useIndex) {
    this.keyType = keyType;
    this.offheapResourceName = offheapResourceName;
    this.diskResourceName = diskResourceName;
    this.useIndex = useIndex;
  }

  public String getKeyType() { return keyType; }

  public String getOffheapResourceName() {
    return offheapResourceName;
  }

  public String getDiskResourceName() {
    return diskResourceName;
  }

  public boolean useIndex() {
    return useIndex;
  }
}
