package org.apache.nifi.processors;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.StreamCallback;
import org.json.JSONObject;
import org.json.XML;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ConversionHandler {
  public static byte[] stream2byteArray(InputStream inputStream) throws IOException {
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    byte[] buffer = new byte[1024];
    int length;
    while ((length = inputStream.read(buffer)) != -1) {
      result.write(buffer, 0, length);
    }
    return result.toByteArray();
  }

  public static String xml2Json(String xml) throws Exception {
    try {
      JSONObject xmlJSONObj = XML.toJSONObject(xml);
      return xmlJSONObj.toString();
    } catch (Exception e) {
      throw new Exception("Error in XML to JSON conversion: " + e.getMessage());
    }
  }

  public static FlowFile convertFlowFile(
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
          throw new IOException("Failed xml conversion!");
        }
      }
    });
    return flowFile;
  }
}
