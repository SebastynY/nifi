package org.apache.nifi.processors;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

@Tags({"example"})
@CapabilityDescription("Provide a description")
public class LogProcessorMost extends AbstractProcessor {

  public static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder()
          .name("success")
          .description("Successful files")
          .build();

  public static final PropertyDescriptor LOG_LEVEL = new PropertyDescriptor.Builder()
          .name("Log Level")
          .description("The log level to use when logging the FlowFiles")
          .required(true)
          .allowableValues("INFO", "DEBUG", "ERROR")
          .defaultValue("INFO")
          .build();

  public static final PropertyDescriptor MESSAGE_PREFIX = new PropertyDescriptor.Builder()
          .name("Message Prefix")
          .description("This string will be prepended to the log message")
          .required(false)
          .defaultValue("LogProcessorMost: ")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) {
    // Custom code to process a FlowFile
  }

  @Override
  public Set<Relationship> getRelationships() {
    return Collections.singleton(SUCCESS_RELATIONSHIP);
  }

  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return Arrays.asList(LOG_LEVEL, MESSAGE_PREFIX);
  }
}
