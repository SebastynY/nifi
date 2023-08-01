package org.apache.nifi.processors;

public class ResponseHandler {
  public static String getResponse(String protocol, String code) {
    if (protocol == null || code == null) {
      throw new IllegalArgumentException("Both protocol and code must be provided");
    }

    if (protocol.equalsIgnoreCase("XI")) {
      if (code.equals("200")) {
        return "XI_OK";
      } else {
        return "Unrecognized XI code";
      }
    } else {
      return code;
    }
  }
}
