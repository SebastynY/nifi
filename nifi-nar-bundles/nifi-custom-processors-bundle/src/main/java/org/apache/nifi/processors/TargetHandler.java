package org.apache.nifi.processors;

import org.json.JSONArray;
import org.json.JSONObject;

public class TargetHandler {
  public static int findTarget(JSONArray targets, String id) {
    for (int i = 0; i < targets.length(); i++) {
      JSONObject target = targets.getJSONObject(i);
      if (target.getString("id").equals(id)) {
        return i;
      }
    }
    return -1;
  }
}
