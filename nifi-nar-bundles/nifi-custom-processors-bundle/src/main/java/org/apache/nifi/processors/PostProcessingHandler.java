package org.apache.nifi.processors;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.json.JSONObject;

import static org.apache.nifi.processors.LogHandler.trace;

public class PostProcessingHandler {
  public static FlowFile postprocessXform(
          FlowFile flowFile,
          boolean syncStatus,
          JSONObject config,
          final ProcessSession session,
          ProcessContext context) throws Exception {
    if (syncStatus) {
      trace("flowfile marked as SYNC, response flow name is: " + config.getString("response"));
      flowFile = session.putAttribute(flowFile, "iflow.status.code", "");
      flowFile = session.putAttribute(flowFile, "business.process.name", config.getString("response"));
    }

    trace("Before potential problem");
    if (flowFile == null) {
      trace("A eto null");
    }
    flowFile = session.putAttribute(flowFile, "iflow.input", config.getString("input"));
    flowFile = session.putAttribute(flowFile, "iflow.sync", config.getString("sync"));
    flowFile = session.putAttribute(flowFile, "processGroupId", "2fde38c3-b6b5-1fee-0c7c-7c06e1792e1a");
    if (context.getProperty(config.getString("id")).getValue() == null) {
//      logger.error("Property for " + config.getString("id") + " not found, add it to processor parameters!");
      flowFile = session.putAttribute(flowFile, "target_system", config.getString("id"));
    }
    return flowFile;
  }
}
