package org.terracotta.tinypounder;

import java.net.Socket;

public class TerracottaServerBusiness {


  public static boolean seemsAvailable(String hostPort) {
    String[] split = hostPort.split(":");
    try (Socket ignored = new Socket(split[0], Integer.parseInt(split[1]))) {
      return true;
    } catch (Exception ignored) {
      return false;
    }
  }

}
