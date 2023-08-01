package org.apache.nifi.processors;

public class LogHandler {
  private static int traceCount1 = 0;
  private static String traceOut1 = "";
  private static int traceCount = 0;
  private static String traceOut = "";

  public static void trace(String message) {
    traceOut += "\r\n+++++++ " + (traceCount++) + " +++++++:" + message;
  }

  public static void trace1(String message) {
    traceOut1 += "\r\n+++++++ " + (traceCount1++) + " +++++++:" + message;
  }
}
