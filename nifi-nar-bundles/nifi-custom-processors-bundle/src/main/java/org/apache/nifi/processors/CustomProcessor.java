package org.apache.nifi.processors;


import org.apache.nifi.flowfile.FlowFile;
import org.apache.commons.io.IOUtils;

import java.nio.charset.StandardCharsets;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.List;

@Tags({"example", "custom", "log", "demo"})
@CapabilityDescription("Logs the content of the flow file.")
public class CustomProcessor extends AbstractProcessor {

  public static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder()
          .name("success")
          .description("All FlowFiles that are read from S3 are routed to this relationship")
          .build();

  private Set<Relationship> relationships;

  @Override
  protected void init(final ProcessorInitializationContext context) {
    final Set<Relationship> relationships = new HashSet<>();
    relationships.add(SUCCESS_RELATIONSHIP);
    this.relationships = Collections.unmodifiableSet(relationships);
  }

  @Override
  public Set<Relationship> getRelationships() {
    return this.relationships;
  }

  @OnScheduled
  public void onScheduled(final ProcessContext context) {
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    final List<FlowFile> flowFiles = session.get(100);
    if (flowFiles.isEmpty()) {
      return;
    }

    for (FlowFile flowFile : flowFiles) {
      session.read(flowFile, new InputStreamCallback() {
        @Override
        public void process(InputStream in) throws IOException {
          String content = IOUtils.toString(in, StandardCharsets.UTF_8);
          getLogger().info("FlowFile content: " + content);
        }
      });

      session.transfer(flowFile, SUCCESS_RELATIONSHIP);
    }
  }
}
