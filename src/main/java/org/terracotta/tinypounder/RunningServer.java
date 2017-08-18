package org.terracotta.tinypounder;

import com.vaadin.ui.TextArea;

import java.io.File;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Mathieu Carbou
 */
class RunningServer {

  private static final Pattern PID_PATTERN = Pattern.compile("PID is ([0-9]*)");

  private final File workDir;
  private final File stripeconfig;
  private final String serverName;
  private final TextArea console;
  private final ArrayBlockingQueue<String> lines;
  private final Runnable onStop;
  private long pid;

  RunningServer(File workDir, File stripeconfig, String serverName, TextArea console, int maxLines, Runnable onStop) {
    this.workDir = workDir;
    this.stripeconfig = stripeconfig;
    this.serverName = serverName;
    this.lines = new ArrayBlockingQueue<>(maxLines);
    this.console = console;
    this.onStop = onStop;
  }

  void stop() {
    try {
      ProcUtils.kill(pid);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  void start() {
    String script = new File(workDir, "server/bin/start-tc-server." + (ProcUtils.isWindows() ? "bat" : "sh")).getAbsolutePath();
    String command = script + " -f " + stripeconfig.getAbsolutePath() + " -n " + serverName;
    ProcUtils.run(
        workDir,
        command,
        lines,
        newLine -> {
          if (pid <= 0 && newLine.contains("INFO com.tc.server.TCServerMain - PID is ")) {
            Matcher m = PID_PATTERN.matcher(newLine);
            if (m.find()) {
              try {
                pid = Long.parseLong(m.group(1));
              } catch (NumberFormatException e) {
                e.printStackTrace();
              }
            }
          }
        },
        onStop);
  }

  void refreshConsole() {
    String text = String.join("\n", lines);
    console.setValue(text);
    console.setCursorPosition(text.length());
  }

}
