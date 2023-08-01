package org.apache.nifi.processors;

import org.apache.nifi.flowfile.FlowFile;
import org.json.JSONObject;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;


public class GraylogHandler {
  private static final String processGroupId = "60484390-d08c-1fe2-b9a9-d47458b352ee";
  private static final String processGroupName = "Transform";

  private static String getSenderService(FlowFile flowFile) {
    String sender = flowFile.getAttribute("http.query.param.senderService");
    return (sender != null) ? sender : "Не указан";
  }

  private static String getHostName() throws UnknownHostException {
    return InetAddress.getLocalHost().getHostName();
  }

  private static String getRequestUri(FlowFile flowFile) {
    String requestUri = flowFile.getAttribute("http.request.uri");
    if ("/sap/xi".equals(requestUri)) {
      requestUri = flowFile.getAttribute("sap.Interface.value");
    }
    return requestUri;
  }

  private static String getReceiver(FlowFile flowFile) {
    String receiver = "Не определен";
    if ("attribute".equals(receiver)) {
      receiver = flowFile.getAttribute("Receiver");
    }
    if (receiver == null) {
      receiver = "Не определен";
    }
    return receiver;
  }

  private static String getBusinessProcessName(FlowFile flowFile) {
    return flowFile.getAttribute("business.process.name");
  }

  private static String getSpecUrl(String businessProcessName) {
    return "https://1desb-s-app01.gk.rosatom.local/nifi-docs/components/ru.greenatom.atombridge/af-specification-nar/4.0.0.0/ru.greenatom.af.Specifications/index.html#" + businessProcessName;
  }

  private static JSONObject createLogEntryJSONObject(String sender, String hostName, String fileName, String uuid, String pathVal, String requestUri, String receiver, String shortMessage) {
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

  public static void graylogNotify(FlowFile flowFile, String xformEntity) throws Exception {
    String sender = getSenderService(flowFile);
    String hostName = getHostName();
    String fileName = flowFile.getAttribute("filename");
    String uuid = flowFile.getAttribute("uuid");
    String pathVal = flowFile.getAttribute("path");
    String requestUri = getRequestUri(flowFile);
    String receiver = getReceiver(flowFile);
    String businessProcessName = getBusinessProcessName(flowFile);
    String specUrl = getSpecUrl(businessProcessName);

    String shortMessage = "Сообщение в [" + processGroupName + "] c filename [" + fileName + "], бизнес-процесс [" + businessProcessName + "], отправитель [" + sender + "], получатель [" + receiver + ']';

    JSONObject map = createLogEntryJSONObject(sender, hostName, fileName, uuid, pathVal, requestUri, receiver, shortMessage);
    map.put("_entryType", "processing");
    map.put("_businessProcess", businessProcessName);
    map.put("specification", specUrl);
    map.put("transformationEntity", xformEntity);

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

  public static void graylogNotifyStart(FlowFile flowFile, String derivationId) throws Exception {
    String sender = getSenderService(flowFile);
    String hostName = getHostName();
    String fileName = flowFile.getAttribute("filename");
    String uuid = flowFile.getAttribute("uuid");
    String pathVal = flowFile.getAttribute("path");
    String requestUri = getRequestUri(flowFile);
    String receiver = getReceiver(flowFile);
    String businessProcessName = getBusinessProcessName(flowFile);
    String specUrl = getSpecUrl(businessProcessName);

    String shortMessage = "Сообщение в [" + processGroupName + "] c filename [" + fileName + "], бизнес-процесс [" + businessProcessName + "], отправитель [" + sender + "], получатель [" + receiver + ']';

    JSONObject map = createLogEntryJSONObject(sender, hostName, fileName, uuid, pathVal, requestUri, receiver, shortMessage);
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
}
