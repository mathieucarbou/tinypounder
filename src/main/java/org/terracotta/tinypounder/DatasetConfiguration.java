package org.terracotta.tinypounder;

public class DatasetConfiguration {
  private final String offheapResourceName;
  private final String diskResourceName;
  private final boolean useIndex;

  public DatasetConfiguration(String offheapResourceName, String diskResourceName, boolean useIndex) {
    this.offheapResourceName = offheapResourceName;
    this.diskResourceName = diskResourceName;
    this.useIndex = useIndex;
  }

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
