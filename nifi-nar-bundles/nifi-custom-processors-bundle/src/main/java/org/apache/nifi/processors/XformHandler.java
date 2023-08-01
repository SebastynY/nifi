package org.apache.nifi.processors;

import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.json.JSONArray;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.apache.nifi.processors.IFlow.REL_SUCCESS;

public class XformHandler {
  static DistributedMapCacheClient xsltCacheMap;
  private static Transformer xslRemoveEnv;

  static void syncResponse(FlowFile flowFile, final ProcessSession session) {
    FlowFile syncResponseFile = session.create(flowFile);
    session.putAttribute(syncResponseFile, "sync.response", "true");
    session.transfer(syncResponseFile, REL_SUCCESS);
  }

  private static void applyXslt(InputStream flowFileContent, OutputStream os, String transformName) throws IOException, IllegalArgumentException, TransformerException {
    if (transformName == null) {
      throw new IOException("XSLT with the name " + transformName + " not found");
    }
    System.out.println("apply xslt transform: " + transformName);

    String xslt = xsltCacheMap.get(transformName, new Serializer<String>() {
      @Override
      public void serialize(String value, OutputStream out) throws IOException {
        out.write(value.getBytes(StandardCharsets.UTF_8));
      }

      public void serialize(OutputStream out, String value) throws IOException {
      }
    }, new Deserializer<String>() {
      public String deserialize(InputStream in) throws IOException, SerializationException {
        return null;
      }

      @Override
      public String deserialize(byte[] value) throws IOException {
        if (value == null) {
          return null;
        }
        return new String(value, StandardCharsets.UTF_8);
      }
    });

    if (xslt == null) {
      System.out.println("transform not found in cache: " + transformName);
      throw new IOException("XSLT with the name " + transformName + " not found");
    }

    Transformer transformer;

    if (transformName.equals("RemoveEnvelope.xsl")) {
      if (xslRemoveEnv != null) {
        transformer = xslRemoveEnv;
      } else {
        transformer = TransformerFactory.newInstance()
                .newTransformer(new StreamSource(new StringReader(xslt)));
        xslRemoveEnv = transformer;
      }
    } else {
      transformer = TransformerFactory.newInstance()
              .newTransformer(new StreamSource(new StringReader(xslt)));
    }

    Writer writer = new OutputStreamWriter(os);
    StreamResult strmres = new StreamResult(writer);
    transformer.transform(new StreamSource(flowFileContent), strmres);
  }

  public static FlowFile processXform(
          FlowFile flowFile,
          JSONArray xforms,
          String targetId,
          final ProcessSession session,
          ProcessContext context) throws Exception {
    boolean isFlowFileSuppressed = false;
    int prevStageIndx = -1;
    session.putAttribute(flowFile, "target.id", targetId);

    if (flowFile.getAttribute("xform.stage") != null) {
      prevStageIndx = Integer.parseInt(flowFile.getAttribute("xform.stage"));
    } else {
      session.putAttribute(flowFile, "xform.stage", "0");
    }

    boolean isPropagated = false;
    int currStageIndx = -1;
    LogHandler.trace1("ID " + targetId);
    LogHandler.trace1("prev stage " + prevStageIndx);
    LogHandler.trace1(" " + xforms);
    session.putAttribute(flowFile, "xform.stage", "0");
    session.getProvenanceReporter().modifyContent(flowFile, "wsrhsrh");

    for (Object xform : xforms) {
      if (isFlowFileSuppressed) {
        return null;
      }
      currStageIndx++;
      session.putAttribute(flowFile, "xform.stage", String.valueOf(currStageIndx));

      if (currStageIndx > prevStageIndx) {
        LogHandler.trace1("Stage " + String.valueOf(currStageIndx));
        String[] nameParamsPair = xform.toString().split("://");
        Map<String, String> params = null;

        if (nameParamsPair.length > 1) {
          params = parseParams(nameParamsPair[1].substring(1));
        }

        for (Map.Entry<String, String> paramEntry : params.entrySet()) {
          LogHandler.trace("Key " + paramEntry.getKey() + " val " + paramEntry.getValue());
        }

        String name = nameParamsPair.length > 1 ? nameParamsPair[0] : xform.toString();
        LogHandler.trace("processing " + String.valueOf(currStageIndx) + " stage");

        switch (name) {
          case "SyncResponse":
            syncResponse(flowFile, session);
            break;
          case "RouteOnContent":
            isPropagated = handleRouteOnContent(params, name, flowFile, isPropagated, session, context);
            break;
          case "UpdateAttribute":
            flowFile = handleUpdateAttribute(params, flowFile, (ProcessContext) session, (ProcessSession) context);
            break;
          case "RouteOnAttribute":
            isFlowFileSuppressed = handleRouteOnAttribute(params, name, flowFile, isFlowFileSuppressed, context);
            break;
          case "ReplaceText":
            flowFile = handleReplaceText(params, name, flowFile, session, context);
            break;
          case "EvaluateXQuery":
            handleEvaluateXQuery(params, name, flowFile, session);
            break;
          case "ApplyXslt":
            flowFile = handleApplyXslt(params, name, flowFile, session);
            break;
          case "DuplicateFlowFile":
            handleDuplicateFlowFile(params, name, flowFile, currStageIndx, xforms, targetId, session, context);
            break;
          default:
            for (Map.Entry<String, String> entry : params.entrySet()) {
              flowFile = session.putAttribute(flowFile, entry.getKey(), entry.getValue());
            }
            session.putAttribute(flowFile, "xform.group", name);
        }

        GraylogHandler.graylogNotify(flowFile, name);
      }

      if (isPropagated) {
        break;
      }
    }

    LogHandler.trace("Stage is " + String.valueOf(currStageIndx) + " size " + String.valueOf(xforms.length()));

    if (currStageIndx == xforms.length() - 1) {
      if (isPropagated) {
        if (!flowFile.getAttribute("target.output").equals("JSON")) {
          session.putAttribute(flowFile, "xform.last", "true");
        }
      } else {
        if (flowFile.getAttribute("target.output").equals("JSON")) {
          flowFile = ConversionHandler.convertFlowFile(flowFile, session);
        }

        flowFile = session.removeAttribute(flowFile, "xform.group");
      }
    }

    LogHandler.trace("Stage is " + flowFile.getAttribute("xform.stage") + " last " + flowFile.getAttribute("xform.last") + " next " + flowFile.getAttribute("xform.next"));

    return flowFile;
  }

  public static boolean handleRouteOnContent(
          Map<String, String> params,
          String name,
          FlowFile flowFile,
          boolean isPropagated,
          final ProcessSession session,
          ProcessContext context) {
    String param = params.get("MatchRequirement");
    if (param == null) {
      throw new IllegalArgumentException(name + " MatchRequirement");
    }
    flowFile = session.putAttribute(flowFile, "content.match.strategy", param);
    param = params.get("RouteCondition");
    if (param == null) {
      throw new IllegalArgumentException(name + " RouteCondition");
    }
    PropertyValue propValue = context.newPropertyValue(param);
    String s = propValue.evaluateAttributeExpressions(flowFile).getValue();
    flowFile = session.putAttribute(flowFile, "route.on.content.condition", s);
    param = params.get("Result");
    if (param == null) {
      throw new IllegalArgumentException(name + " Result");
    }
    propValue = context.newPropertyValue(param);
    s = propValue.evaluateAttributeExpressions(flowFile).getValue();
    flowFile = session.putAttribute(flowFile, "route.on.content.result", s);
    flowFile = session.putAttribute(flowFile, "xform.group", "RouteOnContent");
    isPropagated = true;
    return isPropagated;
  }

  public static FlowFile handleUpdateAttribute(
          Map<String, String> params,
          FlowFile flowFile,
          ProcessContext context,
          final ProcessSession session) {
    for (Map.Entry<String, String> entry : params.entrySet()) {
      PropertyValue propValue = context.newPropertyValue(entry.getValue());
      String attrValue = propValue.evaluateAttributeExpressions(flowFile).getValue();
      flowFile = session.putAttribute(flowFile, entry.getKey(), attrValue);
    }
    return flowFile;
  }

  public static boolean handleRouteOnAttribute(
          Map<String, String> params,
          String name,
          FlowFile flowFile,
          boolean isFlowFileSuppressed,
          ProcessContext context) {
    String param = params.get("RoutingStrategy");
    if (param == null) {
      throw new IllegalArgumentException(name + " RoutingStrategy");
    }
    param = params.get("Condition");
    if (param == null) {
      throw new IllegalArgumentException(name + " Condition");
    }
    PropertyValue propValue = context.newPropertyValue(param);
    String res = propValue.evaluateAttributeExpressions(flowFile).getValue();
    if ("false".equals(res)) {
      isFlowFileSuppressed = true;
    }
    return isFlowFileSuppressed;
  }

  public static FlowFile handleReplaceText(Map<String, String> params, String name, FlowFile flowFile, final ProcessSession session, ProcessContext context) throws Exception {
    String replacementStrategy = params.get("ReplacementStrategy");
    if (replacementStrategy == null) {
      throw new IllegalArgumentException(name + " ReplacementStrategy");
    }
    flowFile = session.putAttribute(flowFile, "replace.text.mode", replacementStrategy);

    String searchValue = params.get("SearchValue");
    if (searchValue == null) {
      throw new IllegalArgumentException(name + " SearchValue");
    }
    PropertyValue propValue = context.newPropertyValue(searchValue);
    searchValue = propValue.evaluateAttributeExpressions(flowFile).getValue();
    flowFile = session.putAttribute(flowFile, "replace.text.search.value", searchValue);

    String replacementValue = params.get("ReplacementValue");
    if (replacementValue == null) {
      throw new IllegalArgumentException(name + " ReplacementValue");
    }
    propValue = context.newPropertyValue(replacementValue);
    replacementValue = propValue.evaluateAttributeExpressions(flowFile).getValue();
    flowFile = session.putAttribute(flowFile, "replace.text.replacement.value", replacementValue);

    int fileSize = (int) flowFile.getSize();
    // Replace the following line with the actual implementation of the replaceText method
    flowFile = TextReplacementHandler.replaceText(flowFile, replacementStrategy, searchValue, replacementValue, "EntireText", StandardCharsets.UTF_8, fileSize, session);

    return flowFile;
  }

  public static void handleEvaluateXQuery(
          Map<String, String> params,
          String name,
          FlowFile flowFile,
          final ProcessSession session) {
    String param = params.get("Destination");
    if (!"flowfile-attribute".equals(param)) {
      throw new IllegalArgumentException(name + " " + param);
    }
    params.remove("Destination");
    for (Map.Entry<String, String> paramEntry : params.entrySet()) {
      LogHandler.trace("Processing " + paramEntry.getKey());
      if (paramEntry.getValue().contains("count(")) {
        LogHandler.trace("+count");
        final StringBuilder sb = new StringBuilder();
        session.read(flowFile, new InputStreamCallback() {
          @Override
          public void process(InputStream is) throws IOException {
            String r = null;
            try {
              r = XPathHandler.evaluateXPathValue(is, paramEntry.getValue().replace("\\\\", "\\"));
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
            sb.append(r);
          }
        });
        session.putAttribute(flowFile, paramEntry.getKey(), sb.toString());
      } else {
        final List<String> list = new ArrayList<>();
        session.read(flowFile, new InputStreamCallback() {
          @Override
          public void process(InputStream is) throws IOException {
            List<Object> nodes = null;
            try {
              nodes = Collections.singletonList(XPathHandler.evaluateXPath(is, paramEntry.getValue().replace("\\\\", "\\")));
            } catch (Exception e) {
              throw new RuntimeException(e);
            }

            for (Object node : nodes) {
              list.add(node.toString());
            }
          }
        });

        LogHandler.trace1("+res");
        if (list.size() == 1) {
          session.putAttribute(flowFile, paramEntry.getKey(), list.get(0));
          LogHandler.trace1("EvalXq res " + paramEntry.getKey() + " " + list.get(0));
        } else {
          int sfx = 1;
          for (String s : list) {
            String attrName = paramEntry.getKey() + "." + sfx;
            LogHandler.trace1("EvalXq res " + attrName + " " + s);
            session.putAttribute(flowFile, attrName, s);
            sfx++;
          }
        }
      }
    }

  }

  public static FlowFile handleApplyXslt(
          Map<String, String> params,
          String name,
          FlowFile flowFile,
          final ProcessSession session) {
    final String param = params.get("Name");
    if (param == null) {
      throw new IllegalArgumentException(name + " " + param);
    }
    flowFile = session.write(flowFile, new StreamCallback() {
      @Override
      public void process(InputStream is, OutputStream os) throws IOException {
        try {
          applyXslt(is, os, param);
        } catch (TransformerException e) {
          throw new RuntimeException(e);
        }
        os.flush();
      }
    });
    return flowFile;
  }

  public static void handleDuplicateFlowFile(
          Map<String, String> params,
          String name,
          FlowFile flowFile,
          int currStageIndx,
          JSONArray xforms,
          String targetId,
          final ProcessSession session,
          ProcessContext context) throws Exception {
    String param = params.get("Number");
    if (param == null) throw new IllegalArgumentException(name + ' ' + param);
    PropertyValue propValue = context.newPropertyValue(param);
    param = propValue.evaluateAttributeExpressions(flowFile).getValue();
    int numOfCopies = Integer.parseInt(param);
    ArrayList<FlowFile> res = new ArrayList<>();
    flowFile = session.putAttribute(flowFile, "copy.index", "0");
    if (currStageIndx == xforms.length() - 1) {
      flowFile = session.removeAttribute(flowFile, "xform.group");
    }
    String ffid = flowFile.getAttribute("uuid");
    for (int i = 0; i < numOfCopies; i++) {
      FlowFile f = session.clone(flowFile);
      f = session.putAttribute(f, "copy.index", String.valueOf(i + 1));
      GraylogHandler.graylogNotifyStart(f, ffid);
      FlowFile ff = null;
      if (currStageIndx < xforms.length() - 1) {
        ff = processXform(f, xforms, targetId, session, context);
      }
      if (ff == null) {
        session.remove(f);
      } else {
        res.add(ff);
      }
    }
    FlowFile ff = null;
    if (currStageIndx < xforms.length() - 1) {
      ff = processXform(flowFile, xforms, targetId, session, context);
      if (ff == null) {
        session.remove(flowFile);
      } else {
        res.add(ff);
      }
    } else {
      res.add(flowFile);
    }
  }

  public static Map<String, String> parseParams(String url) throws Exception {
    Map<String, String> params = new HashMap<>();
    String[] keyValuePairs = url.split("&");

    for (String pair : keyValuePairs) {
      String[] keyValuePair = pair.split("=");
      if (keyValuePair.length > 1) {
        params.put(keyValuePair[0].trim(), keyValuePair[1].trim());
      }
    }
    return params;
  }
}
