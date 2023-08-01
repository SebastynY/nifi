package org.apache.nifi.processors;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.processor.*;
import org.apache.nifi.provenance.ProvenanceReporter;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.Level;

import static org.apache.nifi.processors.LogHandler.trace;

@Tags({"xslt", "xsd", "json", "xml"})
@CapabilityDescription("IFlow processor")
public class IFlow extends AbstractProcessor {

  protected ComponentLog logger = getLogger();
  ProcessContext context;

  DistributedMapCacheClient iflowCacheMap;
  DistributedMapCacheClient xsdCacheMap;
  DistributedMapCacheClient xsltCacheMap;

  private Transformer xslRemoveEnv;


  public static final Relationship REL_SUCCESS = new Relationship.Builder()
          .name("success")
          .description("If the cache was successfully communicated with it will be routed to this relationship")
          .build();

  public static final Relationship REL_FAILURE = new Relationship.Builder()
          .name("failure")
          .description("If unable to communicate with the cache or if the cache entry is evaluated to be blank, the FlowFile will be penalized and routed to this relationship")
          .build();

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


  protected List<PropertyDescriptor> descriptors;
  private Set<Relationship> relationships;

  @Override
  protected void init(final ProcessorInitializationContext context) {
    final List<PropertyDescriptor> descriptors = new ArrayList<>();

    this.descriptors = Collections.unmodifiableList(descriptors);

    final Set<Relationship> relationships = new HashSet<>();
    relationships.add(REL_SUCCESS);
    relationships.add(REL_FAILURE);
    this.relationships = Collections.unmodifiableSet(relationships);
  }

  @Override
  public Set<Relationship> getRelationships() {
    return this.relationships;
  }

  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    final List<PropertyDescriptor> descriptors = new ArrayList<>();
    descriptors.add(IFLOW_DMC_SERVICE);
    descriptors.add(XSD_DMC_SERVICE);
    descriptors.add(XSLT_DMC_SERVICE);
    return descriptors;
  }

  @OnScheduled
  public void onScheduled(final ProcessContext context) {
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) {

    FlowFile flowFile = session.create();
    flowFile = session.putAttribute(flowFile, "hello", "world");
    session.transfer(flowFile, REL_SUCCESS);

    try {
      run(session, context);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  void run(final ProcessSession session, ProcessContext context) throws Exception {


    FlowFile flowFile = session.get();
    if (flowFile == null) {
      return;
    }
    iflowCacheMap = (DistributedMapCacheClient) ControllerServiceHandler.getServiceController("IFLOW_DMC_SERVICE");
    xsdCacheMap = (DistributedMapCacheClient) ControllerServiceHandler.getServiceController("XSD_DMC_SERVICE");
    xsltCacheMap = (DistributedMapCacheClient) ControllerServiceHandler.getServiceController("XSLT_DMC_SERVICE");


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
    String key = flowFile.getAttribute("business.process.name");
    String ret = null;


    try {
      ret = iflowCacheMap.get(key, new Serializer<String>() {
        @Override
        public void serialize(String value, OutputStream out) throws IOException {
          out.write(value.getBytes(StandardCharsets.UTF_8));
        }


        public void serialize(OutputStream out, String value) throws IOException {

        }
      }, new Deserializer<String>() {

        public String deserialize(InputStream in) throws SerializationException {
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


      if (ret == null) {
        trace("iFlow not found, return 501");
        logger.error("iFlow named:" + flowFile.getAttribute("business.process.name") + " not found!");
        flowFile = session.putAttribute(flowFile, "iflow.error", "iFlow named:" + flowFile.getAttribute("business.process.name") + " not found!");
        flowFile = session.putAttribute(flowFile, "iflow.status.code", ResponseHandler.getResponse("", "501"));
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
          int targetIndex = TargetHandler.findTarget(targets, targetId);
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
            Object result = XformHandler.processXform(flowFile, xform, targetId, session, context);
            if (result == null) {
              trace("-ff");
              session.remove(flowFile);
            } else {
              List<String> urlList = target.get("targetPath") instanceof List ? (List<String>) target.get("targetPath") : new ArrayList<String>(Arrays.asList(target.get("targetPath").toString()));
              TransferHandler.transferResult(result, sync, urlList, target, session, context);
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
        key = iflow.getString("validate");
        try {
          schemaContent = xsdCacheMap.get(key, new Serializer<String>() {
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
            XformHandler.syncResponse(file, session);
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

              FlowFile result = XformHandler.processXform(f, xform, target.get("id").toString(), session, context);
              reporter.modifyContent(f);
              if (result == null) {
                session.remove(f);
                break;
              } else {
                List<String> urlList = target.get("targetPath") instanceof List ? (List<String>) target.get("targetPath") : Collections.singletonList(target.get("targetPath").toString());
                TransferHandler.transferResult(result, sync, urlList, target, session, context);
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


}


