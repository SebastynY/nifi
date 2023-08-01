package org.apache.nifi.processors;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

import static org.apache.nifi.processors.IFlow.REL_SUCCESS;

public class TransferHandler {
  public static void transferResult(
          Object result,
          boolean sync,
          List<String> urlList,
          JSONObject config,
          final ProcessSession session,
          final ProcessContext context) throws Exception {
    int urlListSize = urlList.size();
    LogHandler.trace("Medved");

    if (result == null) LogHandler.trace("A result to null");

    if (result instanceof FlowFile) {
      LogHandler.trace("Single");
      FlowFile file = (FlowFile) result;
      file = PostProcessingHandler.postprocessXform(file, sync, config, session, context);
      LogHandler.trace("After postprocess");
      for (int i = 0; i < urlListSize; i++) {
        if (i < urlListSize - 1) {
          FlowFile f = session.clone(file);
          session.putAttribute(f, "target_url", String.valueOf(urlList.get(i)));
          session.transfer(f, REL_SUCCESS);
        } else {
          if (file == null) LogHandler.trace("Why null?");
          session.putAttribute(file, "target_url", String.valueOf(urlList.get(i)));
          session.transfer(file, REL_SUCCESS);
        }
      }
    } else if (result instanceof ArrayList) {
      LogHandler.trace("array");
      ArrayList<FlowFile> list = (ArrayList<FlowFile>) result;
      LogHandler.trace(String.valueOf(list.size()));
      for (FlowFile f : list) {
        if (f == null) continue;
        FlowFile f1 = PostProcessingHandler.postprocessXform(f, sync, config, session, context);

        for (int i = 0; i < urlListSize; i++) {
          if (i < urlListSize - 1) {
            FlowFile fc = session.clone(f1);
            session.putAttribute(fc, "target_url", String.valueOf(urlList.get(i)));
            session.transfer(fc, REL_SUCCESS);
          } else {
            session.putAttribute(f, "target_url", String.valueOf(urlList.get(i)));
            session.transfer(f1, REL_SUCCESS);
          }
        }
      }
    }
  }
}
