package org.apache.nifi.processors;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.lookup.StringLookupService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.provenance.ProvenanceReporter;
import org.apache.nifi.stream.io.StreamUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NewProcessor extends AbstractProcessor {


  ProcessContext context;
  final ComponentLog logger = getLogger();
  final DistributedMapCacheClient IflowMapCacheLookupClient = context.getProperty(IFLOW_DMC_SERVICE).asControllerService(DistributedMapCacheClient.class);
  final DistributedMapCacheClient XsdMapCacheLookupClient = context.getProperty(XSD_DMC_SERVICE).asControllerService(DistributedMapCacheClient.class);
  final DistributedMapCacheClient XsltMapCacheLookupClient = context.getProperty(XSLT_DMC_SERVICE).asControllerService(DistributedMapCacheClient.class);
  private int traceCount1 = 0;
  private String traceOut1 = "";
  private int traceCount = 0;
  private String traceOut = "";

  private Transformer xslRemoveEnv;
  private Map<String, String> xsltCacheMap = new HashMap<>();
  private Map<String, String> iflowCacheMap = new HashMap<>();
  private Map<String, String> xsdCacheMap = new HashMap<>();

  public static final PropertyDescriptor IFLOW_DMC_SERVICE = new PropertyDescriptor.Builder()
          .name("Iflow Distributed Map Cache Client Service")
          .description("The Controller Service for the Iflow Map Cache.")
          .required(true)
          .identifiesControllerService(DistributedMapCacheClient.class)
          .build();


  public static final PropertyDescriptor XSD_DMC_SERVICE = new PropertyDescriptor.Builder()
          .name("XSD Distributed Map Cache Client Service")
          .description("The Controller Service for the XSD Map Cache.")
          .required(true)
          .identifiesControllerService(DistributedMapCacheClient.class)
          .build();

  public static final PropertyDescriptor XSLT_DMC_SERVICE = new PropertyDescriptor.Builder()
          .name("XSLT Distributed Map Cache Client Service")
          .description("The Controller Service for the XSLT Map Cache.")
          .required(true)
          .identifiesControllerService(DistributedMapCacheClient.class)
          .build();


  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    final List<PropertyDescriptor> descriptors = new ArrayList<>();
    descriptors.add(IFLOW_DMC_SERVICE);
    return descriptors;
  }

  public static final Relationship REL_FAILURE = new Relationship.Builder()
          .name("failure")
          .description("If unable to communicate with the cache or if the cache entry is evaluated to be blank, the FlowFile will be penalized and routed to this relationship")
          .build();


  public static final Relationship REL_SUCCESS = new Relationship.Builder()
          .name("success")
          .description("If the cache was successfully communicated with it will be routed to this relationship")
          .build();

//  public NewProcessor(ProcessContext context) {
//    this.context = context;
//  }

  public void trace(String message) {
    traceOut += "\r\n+++++++ " + (traceCount++) + " +++++++:" + message;
    // logger.log(LogLevel.INFO, message);
  }

  public void trace1(String message) {
    traceOut1 += "\r\n+++++++ " + (traceCount1++) + " +++++++:" + message;
    // logger.log(LogLevel.INFO, message);
  }

  public ControllerService getServiceController(String name) {
    logger.info("get service controller: " + name);
    ControllerServiceLookup lookup = context.getControllerServiceLookup();
    Set<String> serviceIds = lookup.getControllerServiceIdentifiers(ControllerService.class);
    String foundServiceId = null;
    for (String serviceId : serviceIds) {
      if (lookup.getControllerServiceName(serviceId).equals(name)) {
        foundServiceId = serviceId;
        break;
      }
    }
    return lookup.getControllerService(foundServiceId);
  }

  public String getResponse(String protocol, String code) {
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

  int findTarget(JSONArray targets, String id) {
    for (int i = 0; i < targets.length(); i++) {
      JSONObject target = targets.getJSONObject(i);
      if (target.getString("id").equals(id)) {
        return i;
      }
    }
    return -1;
  }

  private String evaluateXPathValue(InputStream inputStream, String xpathQuery) throws Exception {
    DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    Document document = builder.parse(inputStream);

    XPathFactory xPathFactory = XPathFactory.newInstance();
    XPath xPath = xPathFactory.newXPath();
    XPathExpression xPathExpression = xPath.compile(xpathQuery);

    return xPathExpression.evaluate(document);
  }

  public FlowFile replaceText(
          final FlowFile flowFile,
          final String type,
          final String searchValue,
          final String replacementValue,
          final String evaluateMode,
          final Charset charset,
          final int maxBufferSize,
          final ProcessSession session) throws Exception {
    if (type.equals("RegexReplace")) {
      return regexReplaceText(flowFile, searchValue, replacementValue, evaluateMode, charset, maxBufferSize, session);
    } else {
      throw new Exception("Incorrect replace strategy");
    }
  }

  void syncResponse(
          FlowFile flowFile,
          final ProcessSession session) {
    FlowFile syncResponseFile = session.create(flowFile);
    session.putAttribute(syncResponseFile, "sync.response", "true");
    session.transfer(syncResponseFile, REL_SUCCESS);
  }

  public Map<String, String> parseParams(String url) throws Exception {
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

  private static String normalizeReplacementString(String replacement) {
    String replacementFinal = replacement;
    if (Pattern.compile("(\\$\\D)").matcher(replacement).find()) {
      replacementFinal = Matcher.quoteReplacement(replacement);
    }
    return replacementFinal;
  }

  public FlowFile regexReplaceText(
          final FlowFile flowFile,
          final String searchValue,
          final String replacementValue,
          final String evaluateMode,
          final Charset charset,
          final int maxBufferSize,
          final ProcessSession session) throws Exception {
    final int numCapturingGroups = Pattern.compile(searchValue).matcher("").groupCount();

    final Pattern searchPattern = Pattern.compile(searchValue);
    final Map<String, String> additionalAttrs = new HashMap<>(numCapturingGroups);

    if (evaluateMode.equalsIgnoreCase("EntireText")) {
      final int flowFileSize = (int) flowFile.getSize();
      final int bufferSize = Math.min(maxBufferSize, flowFileSize);
      final byte[] buffer = new byte[bufferSize];

      session.read(flowFile, new InputStreamCallback() {
        @Override
        public void process(InputStream is) throws IOException {
          StreamUtils.fillBuffer(is, buffer, false);
        }
      });

      final String contentString = new String(buffer, 0, flowFileSize, charset);
      final Matcher matcher = searchPattern.matcher(contentString);

      int matches = 0;
      final StringBuffer sb = new StringBuffer();
      while (matcher.find()) {
        matches++;

        for (int i = 0; i <= matcher.groupCount(); i++) {
          additionalAttrs.put("$" + i, matcher.group(i));
        }

        String replacementFinal = normalizeReplacementString(replacementValue);

        matcher.appendReplacement(sb, replacementFinal);
      }

      if (matches > 0) {
        matcher.appendTail(sb);

        final String updatedValue = sb.toString();
        return session.write(flowFile, new OutputStreamCallback() {
          @Override
          public void process(OutputStream os) throws IOException {
            os.write(updatedValue.getBytes(charset));
          }
        });
      } else {
        return flowFile;
      }
    } else {
      throw new Exception("unsupported evaluation mode");
    }
  }

  public byte[] stream2byteArray(InputStream inputStream) throws IOException {
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    byte[] buffer = new byte[1024];
    int length;
    while ((length = inputStream.read(buffer)) != -1) {
      result.write(buffer, 0, length);
    }
    return result.toByteArray();
  }

  public String xml2Json(String xml) throws Exception {
    try {
      JSONObject xmlJSONObj = XML.toJSONObject(xml);
      return xmlJSONObj.toString();
    } catch (Exception e) {
      throw new Exception("Error in XML to JSON conversion: " + e.getMessage());
    }
  }

  public FlowFile convertFlowFile(
          FlowFile flowFile,
          final ProcessSession session) throws Exception {
    flowFile = session.write(flowFile, new StreamCallback() {
      @Override
      public void process(InputStream is, OutputStream os) throws IOException {
        byte[] content = stream2byteArray(is);
        System.out.println("Len " + content.length); // replace trace
        String json = null;
        try {
          json = xml2Json(new String(content));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        if (json != null) {
          os.write(json.getBytes());
          os.flush();
        } else {
          throw new IOException("Failed xml convertation!");
        }
      }
    });
    return flowFile;
  }

  public List<String> evaluateXPath(InputStream inputStream, String xpathQuery) throws Exception {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = factory.newDocumentBuilder();
    Document doc = builder.parse(inputStream);
    XPathFactory xPathfactory = XPathFactory.newInstance();
    XPath xpath = xPathfactory.newXPath();
    XPathExpression expr = xpath.compile(xpathQuery);
    NodeList nodes = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);

    List<String> nodeListContent = new ArrayList<>();
    for (int i = 0; i < nodes.getLength(); i++) {
      nodeListContent.add(nodes.item(i).getTextContent());
    }
    return nodeListContent;
  }

  public void graylogNotify(FlowFile flowFile, String xformEntity, StringLookupService receiverServiceId) throws Exception {
    String sender = flowFile.getAttribute("http.query.param.senderService");
    if (sender == null) {
      sender = "Не указан";
    }

    String processGroupId = "60484390-d08c-1fe2-b9a9-d47458b352ee";
    String processGroupName = "Transform";
    String hostName = InetAddress.getLocalHost().getHostName();
    String fileName = flowFile.getAttribute("filename");
    String uuid = flowFile.getAttribute("uuid");
    String pathVal = flowFile.getAttribute("path");
    String requestUri = flowFile.getAttribute("http.request.uri");

    if ("/sap/xi".equals(requestUri)) {
      requestUri = flowFile.getAttribute("sap.Interface.value");
    }

    String xformPath = flowFile.getAttribute("xform.path");
    if (xformPath == null) {
      xformPath = "unknow";
    }

    String xformStage = flowFile.getAttribute("xform.stage");
    if (xformStage == null) {
      xformStage = "unknow";
    }

    //Определение получателя
    String receiver = "Не определен";

    if ((StringLookupService) receiverServiceId != null) {
      Map<String, Object> coordinate = new HashMap<>();
      coordinate.put("key", requestUri);
      Optional<String> value = ((StringLookupService) receiverServiceId).lookup(coordinate);
      if (value.isPresent()) {
        receiver = value.get();
      }
    }

    if ("attribute".equals(receiver)) {
      receiver = flowFile.getAttribute("Receiver");
    }
    if (receiver == null) {
      receiver = "Не определен";
    }

    //Определение бизнес-процесса
    String businessProcessName = flowFile.getAttribute("business.process.name");

    String specUrl = "https://1desb-s-app01.gk.rosatom.local/nifi-docs/components/ru.greenatom.atombridge/af-specification-nar/4.0.0.0/ru.greenatom.af.Specifications/index.html#" +
            businessProcessName;

    //Формирование GELF-сообщения
    String shortMessage = "Сообщение в [" + processGroupName + "] c filename [" + fileName + "], бизнес-процесс [" + businessProcessName + "], отправитель [" + sender + "], получатель [" + receiver + ']';

    JSONObject map = createLogEntryJSONObject(sender, processGroupId, processGroupName, hostName, fileName, uuid, pathVal, requestUri, receiver, shortMessage);
    map.put("_entryType", "processing");
    map.put("_businessProcess", businessProcessName);
    map.put("specification", specUrl);
    map.put("transformationEntity", xformEntity);
    map.put("transformationPath", xformPath);
    map.put("transformationStage", xformStage);

    String json = map.toString();

    URL url = new URL("http://1tesb-s-grl01.gk.rosatom.local:12001/gelf");
    HttpURLConnection post = (HttpURLConnection) url.openConnection();
    post.setRequestMethod("POST");
    post.setDoOutput(true);
    post.setRequestProperty("Content-Type", "application/json");
    try (OutputStream os = post.getOutputStream()) {
      byte[] input = json.getBytes(StandardCharsets.UTF_8);
      os.write(input, 0, input.length);
    }

    int postRC = post.getResponseCode();
    if (postRC < 200 || postRC > 300) {
      throw new Exception("Ошибка отправки, код " + postRC);
    }
  }

  public void graylogNotifyStart(FlowFile flowFile, String derivationId, StringLookupService receiverServiceId) throws Exception {
    String sender = flowFile.getAttribute("http.query.param.senderService");
    if (sender == null) {
      sender = "Не указан";
    }

    String processGroupId = "60484390-d08c-1fe2-b9a9-d47458b352ee";
    String processGroupName = "Transform";
    String hostName = InetAddress.getLocalHost().getHostName();
    String fileName = flowFile.getAttribute("filename");
    String uuid = flowFile.getAttribute("uuid");
    String pathVal = flowFile.getAttribute("path");
    String requestUri = flowFile.getAttribute("http.request.uri");

    if ("/sap/xi".equals(requestUri)) {
      requestUri = flowFile.getAttribute("sap.Interface.value");
    }

    //Определение получателя
    String receiver = "Не определен";
    StringLookupService receiverLookup = receiverServiceId;
    if (receiverLookup != null) {
      Map<String, Object> coordinate = new HashMap<>();
      coordinate.put("key", requestUri);
      Optional<String> value = receiverLookup.lookup(coordinate);
      if (value.isPresent()) {
        receiver = value.get();
      }
    }

    if ("attribute".equals(receiver)) {
      receiver = flowFile.getAttribute("Receiver");
    }
    if (receiver == null) {
      receiver = "Не определен";
    }

    //Определение бизнес-процесса
    String businessProcessName = flowFile.getAttribute("business.process.name");

    String specUrl = "https://1desb-s-app01.gk.rosatom.local/nifi-docs/components/ru.greenatom.atombridge/af-specification-nar/4.0.0.0/ru.greenatom.af.Specifications/index.html#" +
            businessProcessName;

    //Формирование GELF-сообщения
    String shortMessage = "Сообщение в [" + processGroupName + "] c filename [" + fileName + "], бизнес-процесс [" + businessProcessName + "], отправитель [" + sender + "], получатель [" + receiver + ']';

    JSONObject map = createLogEntryJSONObject(
            sender, processGroupId, processGroupName, hostName,
            fileName, uuid, pathVal, requestUri, receiver, shortMessage);
    map.put("level", "INFO");
    map.put("_entryType", "start");
    map.put("_LogStart", 1);
    map.put("_LogSuccess", 0);
    map.put("_businessProcess", businessProcessName);
    map.put("specification", specUrl);
    map.put("derivation", derivationId);

    String json = map.toString();

    URL url = new URL("http://1tesb-s-grl01.gk.rosatom.local:12001/gelf");
    HttpURLConnection post = (HttpURLConnection) url.openConnection();
    post.setRequestMethod("POST");
    post.setDoOutput(true);
    post.setRequestProperty("Content-Type", "application/json");
    try (OutputStream os = post.getOutputStream()) {
      byte[] input = json.getBytes(StandardCharsets.UTF_8);
      os.write(input, 0, input.length);
    }

    int postRC = post.getResponseCode();
    if (postRC < 200 || postRC > 300) {
      throw new Exception("Ошибка отправки, код " + postRC);
    }
  }

  static JSONObject createLogEntryJSONObject(String sender, String processGroupId, String processGroupName, String hostName, String fileName, String uuid, String pathVal, String requestUri, String receiver, String shortMessage) {
    JSONObject map = new JSONObject();
    map.put("_fileName", fileName);
    map.put("path", pathVal);
    map.put("short_message", shortMessage);
    map.put("host", hostName);
    map.put("facility", processGroupName);
    map.put("_groupId", processGroupId);
    map.put("level", "INFO");
    map.put("_groupName", processGroupName);
    map.put("_messageUuid", uuid);
    map.put("_requestUrl", requestUri);
    map.put("_sender", sender);
    map.put("_receiver", receiver);
    return map;
  }


  public FlowFile processXform(
          FlowFile flowFile,
          JSONArray xforms,
          String targetId,
          final ProcessSession session,
          ProcessContext context,
          StringLookupService receiverServiceId) throws Exception {
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
    trace1("ID " + targetId);
    trace1("prev stage " + prevStageIndx);
    trace1(" " + xforms);
    session.putAttribute(flowFile, "xform.stage", "0");
    session.getProvenanceReporter().modifyContent(flowFile, "wsrhsrh");

    for (Object xform : xforms) {
      if (isFlowFileSuppressed) {
        return null;
      }
      currStageIndx++;
      session.putAttribute(flowFile, "xform.stage", String.valueOf(currStageIndx));

      if (currStageIndx > prevStageIndx) {
        trace1("Stage " + String.valueOf(currStageIndx));
        String[] nameParamsPair = xform.toString().split("://");
        Map<String, String> params = null;

        if (nameParamsPair.length > 1) {
          params = parseParams(nameParamsPair[1].substring(1));
        }

        for (Map.Entry<String, String> paramEntry : params.entrySet()) {
          trace("Key " + paramEntry.getKey() + " val " + paramEntry.getValue());
        }

        String name = nameParamsPair.length > 1 ? nameParamsPair[0] : xform.toString();
        trace("processing " + String.valueOf(currStageIndx) + " stage");

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
            handleDuplicateFlowFile(params, name, flowFile, currStageIndx, xforms, targetId, session, context, receiverServiceId);
            break;
          default:
            for (Map.Entry<String, String> entry : params.entrySet()) {
              flowFile = session.putAttribute(flowFile, entry.getKey(), entry.getValue());
            }
            session.putAttribute(flowFile, "xform.group", name);
        }

        graylogNotify(flowFile, name, receiverServiceId);
      }

      if (isPropagated) {
        break;
      }
    }

    trace("Stage is " + String.valueOf(currStageIndx) + " size " + String.valueOf(xforms.length()));

    if (currStageIndx == xforms.length() - 1) {
      if (isPropagated) {
        if (!flowFile.getAttribute("target.output").equals("JSON")) {
          session.putAttribute(flowFile, "xform.last", "true");
        }
      } else {
        if (flowFile.getAttribute("target.output").equals("JSON")) {
          flowFile = convertFlowFile(flowFile, session);
        }

        flowFile = session.removeAttribute(flowFile, "xform.group");
      }
    }

    trace("Stage is " + flowFile.getAttribute("xform.stage") + " last " + flowFile.getAttribute("xform.last") + " next " + flowFile.getAttribute("xform.next"));

    return flowFile;
  }

  public void handleDuplicateFlowFile(
          Map<String, String> params,
          String name,
          FlowFile flowFile,
          int currStageIndx,
          JSONArray xforms,
          String targetId,
          final ProcessSession session,
          ProcessContext context,
          StringLookupService receiverServiceId) throws Exception {
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
      graylogNotifyStart(f, ffid, receiverServiceId);
      FlowFile ff = null;
      if (currStageIndx < xforms.length() - 1) {
        ff = processXform(f, xforms, targetId, session, context, receiverServiceId);
      }
      if (ff == null) {
        session.remove(f);
      } else {
        res.add(ff);
      }
    }
    FlowFile ff = null;
    if (currStageIndx < xforms.length() - 1) {
      ff = processXform(flowFile, xforms, targetId, session, context, receiverServiceId);
      if (ff == null) {
        session.remove(flowFile);
      } else {
        res.add(ff);
      }
    } else {
      res.add(flowFile);
    }
  }


  public void applyXslt(InputStream flowFileContent, OutputStream os, String transformName) throws IOException, TransformerException {
    if (transformName == null || transformName.isEmpty()) {
      throw new IOException("XSLT with the name " + transformName + " not found");
    }
    trace("apply xslt transform: " + transformName);

    String xslt = xsltCacheMap.getOrDefault(transformName, null);
    if (xslt == null) {
      trace("transform not found in cache: " + transformName);
      logger.error("transform not found in cache: " + transformName);
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

    Writer writer = new OutputStreamWriter(os, StandardCharsets.UTF_8);
    StreamResult strmres = new StreamResult(writer);
    transformer.transform(new StreamSource(flowFileContent), strmres);
  }

  public FlowFile handleApplyXslt(
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


  public void handleEvaluateXQuery(
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
      trace("Processing " + paramEntry.getKey());
      if (paramEntry.getValue().contains("count(")) {
        trace("+count");
        final StringBuilder sb = new StringBuilder();
        session.read(flowFile, new InputStreamCallback() {
          @Override
          public void process(InputStream is) throws IOException {
            String r = null;
            try {
              r = evaluateXPathValue(is, paramEntry.getValue().replace("\\\\", "\\"));
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
              nodes = Collections.singletonList(evaluateXPath(is, paramEntry.getValue().replace("\\\\", "\\")));
            } catch (Exception e) {
              throw new RuntimeException(e);
            }

            for (Object node : nodes) {
              list.add(node.toString());
            }
          }
        });

        trace1("+res");
        if (list.size() == 1) {
          session.putAttribute(flowFile, paramEntry.getKey(), list.get(0));
          trace1("EvalXq res " + paramEntry.getKey() + " " + list.get(0));
        } else {
          int sfx = 1;
          for (String s : list) {
            String attrName = paramEntry.getKey() + "." + sfx;
            trace1("EvalXq res " + attrName + " " + s);
            session.putAttribute(flowFile, attrName, s);
            sfx++;
          }
        }
      }
    }

  }

  public FlowFile handleReplaceText(Map<String, String> params, String name, FlowFile flowFile, final ProcessSession session, ProcessContext context) throws Exception {
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
    flowFile = replaceText(flowFile, replacementStrategy, searchValue, replacementValue, "EntireText", StandardCharsets.UTF_8, fileSize, session);

    return flowFile;
  }


  public boolean handleRouteOnAttribute(
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


  public FlowFile handleUpdateAttribute(
          Map<String, String> params,
          FlowFile flowFile,
          ProcessContext context,
          final ProcessSession session) {
    // We have to support the Nifi EL in attributes
    // So create temp hidden property to provide EL capabilities
    for (Map.Entry<String, String> entry : params.entrySet()) {
      PropertyValue propValue = context.newPropertyValue(entry.getValue());
      String attrValue = propValue.evaluateAttributeExpressions(flowFile).getValue();
      flowFile = session.putAttribute(flowFile, entry.getKey(), attrValue);
    }
    return flowFile;
  }


  public boolean handleRouteOnContent(
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


  public FlowFile postprocessXform(
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
      logger.error("Property for " + config.getString("id") + " not found, add it to processor parameters!");
      flowFile = session.putAttribute(flowFile, "target_system", config.getString("id"));
    }
    return flowFile;
  }


  void transferResult(
          Object result,
          boolean sync,
          List<String> urlList,
          JSONObject config,
          final ProcessSession session,
          final ProcessContext context) throws Exception {
    int urlListSize = urlList.size();
    trace("Medved");

    if (result == null) trace("A result to null");

    if (result instanceof FlowFile) {
      trace("Single");
      FlowFile file = (FlowFile) result;
      file = postprocessXform(file, sync, config, session, context);
      trace("After postprocess");
      for (int i = 0; i < urlListSize; i++) {
        if (i < urlListSize - 1) {
          FlowFile f = session.clone(file);
          session.putAttribute(f, "target_url", String.valueOf(urlList.get(i)));
          session.transfer(f, REL_SUCCESS);
        } else {
          if (file == null) trace("Why null?");
          session.putAttribute(file, "target_url", String.valueOf(urlList.get(i)));
          session.transfer(file, REL_SUCCESS);
        }
      }
    } else if (result instanceof ArrayList) {
      trace("array");
      ArrayList<FlowFile> list = (ArrayList<FlowFile>) result;
      trace(String.valueOf(list.size()));
      for (FlowFile f : list) {
        if (f == null) continue;
        FlowFile f1 = postprocessXform(f, sync, config, session, context);

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


  void run(final ProcessSession session, ProcessContext context, StringLookupService receiverServiceId) throws Exception {


    FlowFile flowFile = session.get();
    if (flowFile == null) {
      return;
    }


    TransformerFactory xslRemoveEnv = null;
    String traceOut = "";
    int traceCount = 0;
    String traceOut1 = "";
    int traceCount1 = 0;

    XPath xpath = XPathFactory.newInstance().newXPath();
    try {
      DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    } catch (ParserConfigurationException e) {
      throw new RuntimeException(e);
    }

    String iflowMapCacheLookupClientName = IflowMapCacheLookupClient.getPropertyDescriptors().toString();
    String xsdMapCacheLookupClientName = XsdMapCacheLookupClient.getPropertyDescriptors().toString();
    String xsltMapCacheLookupClientName = XsltMapCacheLookupClient.getPropertyDescriptors().toString();


    try {
      iflowCacheMap = (Map<String, String>) getServiceController(iflowMapCacheLookupClientName);
      xsdCacheMap = (Map<String, String>) getServiceController(xsdMapCacheLookupClientName);
      xsltCacheMap = (Map<String, String>) getServiceController(xsltMapCacheLookupClientName);

      String ret = iflowCacheMap.get(
              flowFile.getAttribute("business.process.name")
      );

      if (ret == null) {
        trace("iFlow not found, return 501");
        logger.error("iFlow named:" + flowFile.getAttribute("business.process.name") + " not found!");
        flowFile = session.putAttribute(flowFile, "iflow.error", "iFlow named:" + flowFile.getAttribute("business.process.name") + " not found!");
        flowFile = session.putAttribute(flowFile, "iflow.status.code", getResponse("", "501"));
        session.transfer(flowFile, REL_FAILURE);
        return;
      } else {
        trace("readed iFlow config");
      }
      trace("start parsing json iFlow");

      JSONObject iflow = new JSONObject(ret);
      JSONArray targets = iflow.getJSONArray("targets");
      trace("full list of defined target systems: " + targets);
      boolean sync = Boolean.parseBoolean(iflow.getString("sync"));
      int numOfTargets = targets.length();
      if (flowFile.getAttribute("xform.stage") != null && flowFile.getAttribute("target.id") != null && flowFile.getAttribute("xform.path") != null) {
        try {
          trace("+loopback " + flowFile.getAttribute("xform.stage"));
          String targetId = flowFile.getAttribute("target.id");
          int targetIndex = findTarget(targets, targetId);
          if (targetIndex < 0) {
            throw new IllegalArgumentException("Target not found");
          }
          JSONObject target = targets.getJSONObject(targetIndex);
          JSONArray xforms = target.getJSONArray("transformations");
          int xformPath = Integer.parseInt(flowFile.getAttribute("xform.path"));
          if (xformPath > -1 && xformPath < xforms.length()) {
            if (target.getString("output").equals("JSON")) {
              flowFile = session.putAttribute(flowFile, "target.output", "JSON");
            }
            JSONArray xform = xforms.getJSONArray(xformPath);
            Object result = processXform(flowFile, xform, targetId, session, context, receiverServiceId);
            if (result == null) {
              trace("-ff");
              session.remove(flowFile);
            } else {
              List<String> urlList = target.get("targetPath") instanceof List ? (List<String>) target.get("targetPath") : new ArrayList<String>(Arrays.asList(target.get("targetPath").toString()));
              transferResult(result, sync, urlList, target, session, context);
            }
          } else {
            throw new Exception("Incorrect transformation path " + xformPath);
          }
        } catch (Exception ex1) {
          trace("!!!!!!!Exception: " + ex1.toString());
          String exMsgBldr = "Exception '" + ex1.toString() + "' occurs" +
                  " while processing FlowFile '" + flowFile.getAttribute("filename") + "'" +
                  " in '" + flowFile.getAttribute("business.process.name") + "' scenario" +
                  " at '" + flowFile.getAttribute("target.id") + "' target" +
                  " at '" + flowFile.getAttribute("xform.path") + "' path" +
                  " at " + flowFile.getAttribute("xform.stage") + " stage";
          session.putAttribute(flowFile, "error.msg", ex1.toString());
          logger.error(exMsgBldr, Level.SEVERE);
          logger.error(traceOut, Level.SEVERE);
          session.putAttribute(flowFile, "error.msg", ex1.toString());
          session.transfer(flowFile, REL_FAILURE);
          return;
        }
      } else {
        // Validate against xsd schema
        flowFile = session.putAttribute(flowFile, "xform.stage", "-1");
        String schemaContent = null;
        boolean isFailedSchemaExtraction = false;
        try {
          schemaContent = xsdCacheMap.get(iflow.getString("validate")
          );
          if (schemaContent == null) {
            throw new IOException("Schema with name " + iflow.getString("validate") + " not found");
          }
        } catch (Exception e) {
          String msg = "Failed schema extraction! " + e;
          logger.error(msg);
          session.putAttribute(flowFile, "error.msg", msg);
          session.transfer(flowFile, REL_FAILURE);
          isFailedSchemaExtraction = true;
        }
        if (isFailedSchemaExtraction) return;

        InputStream fis = null;
        boolean isFailedValidation = false;
        try {
          fis = session.read(flowFile);
          Source xmlFile = new StreamSource(fis);
          SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
          Schema schema = schemaFactory.newSchema(new StreamSource(new StringReader(schemaContent)));
          Validator validator = schema.newValidator();
          validator.validate(xmlFile);
        } catch (Exception e) {
          String msg = "Failed xml validation! " + e;
          logger.error(msg);
          session.putAttribute(flowFile, "error.msg", msg);
          isFailedValidation = true;
          session.transfer(flowFile, REL_FAILURE);
        } finally {
          if (fis != null) {
            try {
              fis.close();
            } catch (IOException e) {
              logger.error("Failed to close InputStream", e);
            }
          }
        }
        if (isFailedValidation) return;

        ProvenanceReporter reporter = session.getProvenanceReporter();
        for (int flowIndex = 0; flowIndex < targets.length(); flowIndex++) {
          JSONObject target = targets.getJSONObject(flowIndex);
          JSONArray xforms = target.getJSONArray("transformations");
          FlowFile file;
          if (flowIndex < numOfTargets - 1 & numOfTargets > 1) {
            file = session.clone(flowFile);
            reporter.clone(flowFile, file);
          } else {
            file = flowFile;
          }
          session.putAttribute(file, "Receiver", target.get("id").toString());
          int xformPath = -1;

          if (target.get("SyncValidation").equals("true")) {
            syncResponse(file, session);
          }

          if (target.get("output").equals("JSON")) {
            session.putAttribute(file, "target.output", "JSON");
          }

          FlowFile f = null;
          for (int i = 0; i < xforms.length(); i++) {
            try {
              JSONArray xform = (JSONArray) xforms.get(i);
              xformPath++;
              session.putAttribute(file, "xform.path", String.valueOf(xformPath));
              f = xformPath < xforms.length() - 1 & xforms.length() > 1 ? session.clone(file) : file;

              FlowFile result = processXform(f, xform, target.get("id").toString(), session, context, receiverServiceId);
              reporter.modifyContent(f);
              if (result == null) {
                session.remove(f);
                break;
              } else {
                List<String> urlList = target.get("targetPath") instanceof List ? (List<String>) target.get("targetPath") : Collections.singletonList(target.get("targetPath").toString());
                transferResult(result, sync, urlList, target, session, context);
              }
            } catch (Exception ex1) {
              if (f == null) throw new IllegalStateException("The FlowFile 'f' was not properly initialized.");
              String exMsgBldr = "Exception '" + ex1.toString() + "' occurs" +
                      " while processing FlowFile '" + f.getAttribute("filename") + "'" +
                      " in '" + f.getAttribute("business.process.name") + "' scenario" +
                      " at '" + (String) target.get("id") + "' target" +
                      " at " + f.getAttribute("xform.path") + " path" +
                      " at " + f.getAttribute("xform.stage") + " stage";
              logger.log(LogLevel.ERROR, exMsgBldr);
              logger.log(LogLevel.ERROR, traceOut);

              session.putAttribute(f, "error.msg", ex1.toString());

              session.transfer(f, REL_FAILURE);
            }
          }
        }
      }
    } catch (Exception ex) {
      session.putAttribute(flowFile, "error.msg", ex.toString());
      logger.log(LogLevel.ERROR, traceOut);
      session.transfer(flowFile, REL_FAILURE);
    } finally {
      logger.log(LogLevel.INFO, traceOut1);
    }
  }

  @Override
  public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {

  }
}







