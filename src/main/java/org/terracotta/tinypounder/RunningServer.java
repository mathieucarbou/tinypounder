package org.terracotta.tinypounder;

import com.vaadin.ui.TextArea;

import java.io.File;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Mathieu Carbou
 */
class RunningServer {

  private static final String ACTIVE_PATTERN = "Becoming State[ ACTIVE-COORDINATOR ]";
  private static final Pattern PID_PATTERN = Pattern.compile("PID is ([0-9]*)");
  private static final Pattern PASSIVE_PATTERN = Pattern.compile("Moved to State\\[ (.+) ]");

  private final File workDir;
  private final File stripeconfig;
  private final String serverName;
  private final TextArea console;
  private final ArrayBlockingQueue<String> lines;
  private final Runnable onTerminated;
  private final Consumer<String> onState;
  private final Consumer<Long> onPID;
  private long pid;

  RunningServer(File workDir, File stripeconfig, String serverName, TextArea console, int maxLines, Runnable onTerminated, Consumer<String> onState, Consumer<Long> onPID) {
    this.workDir = workDir;
    this.stripeconfig = stripeconfig;
    this.serverName = serverName;
    this.lines = new ArrayBlockingQueue<>(maxLines);
    this.console = console;
    this.onTerminated = onTerminated;
    this.onState = onState;
    this.onPID = onPID;
  }

  void kill() {
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
          if (newLine.contains(ACTIVE_PATTERN)) {
            onState.accept("ACTIVE");
          } else if (newLine.contains("INFO - Moved to State[")) {
            Matcher m = PASSIVE_PATTERN.matcher(newLine);
            if (m.find()) {
              onState.accept(m.group(1));
            }
          } else if (newLine.contains("INFO com.tc.server.TCServerMain - PID is ")) {
            Matcher m = PID_PATTERN.matcher(newLine);
            if (m.find()) {
              try {
                pid = Long.parseLong(m.group(1));
                onPID.accept(pid);
              } catch (NumberFormatException e) {
                e.printStackTrace();
              }
            }
          }
        },
        onTerminated);
  }

  void refreshConsole() {
    TinyPounderMainUI.updateTextArea(console, lines);
  }

}
