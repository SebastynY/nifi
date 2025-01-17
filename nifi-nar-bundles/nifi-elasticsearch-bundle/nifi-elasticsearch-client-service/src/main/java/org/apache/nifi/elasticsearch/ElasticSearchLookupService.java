/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.JsonInferenceSchemaRegistryService;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@CapabilityDescription("Lookup a record from Elasticsearch Server associated with the specified document ID. " +
        "The coordinates that are passed to the lookup must contain the key 'id'.")
@Tags({"lookup", "enrich", "record", "elasticsearch"})
@DynamicProperty(name = "A JSONPath expression", value = "A Record Path expression",
        description = "Retrieves an object using JSONPath from the result document and places it in the return Record at the specified Record Path.")
public class ElasticSearchLookupService extends JsonInferenceSchemaRegistryService implements LookupService<Record> {
  public static final PropertyDescriptor CLIENT_SERVICE = new PropertyDescriptor.Builder()
          .name("el-rest-client-service")
          .displayName("Client Service")
          .description("An ElasticSearch client service to use for running queries.")
          .identifiesControllerService(ElasticSearchClientService.class)
          .required(true)
          .build();
  public static final PropertyDescriptor INDEX = new PropertyDescriptor.Builder()
          .name("el-lookup-index")
          .displayName("Index")
          .description("The name of the index to read from")
          .required(true)
          .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

  public static final PropertyDescriptor TYPE = new PropertyDescriptor.Builder()
          .name("el-lookup-type")
          .displayName("Type")
          .description("The type of this document (used by Elasticsearch for indexing and searching)")
          .required(false)
          .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

  private ElasticSearchClientService clientService;

  private String index;
  private String type;
  private ObjectMapper mapper;

  private volatile ConcurrentHashMap<String, RecordPath> recordPathMappings;

  private final List<PropertyDescriptor> descriptors;

  public ElasticSearchLookupService() {
    final List<PropertyDescriptor> desc = new ArrayList<>(super.getSupportedPropertyDescriptors());
    desc.add(CLIENT_SERVICE);
    desc.add(INDEX);
    desc.add(TYPE);
    descriptors = Collections.unmodifiableList(desc);
  }

  @Override
  @OnEnabled
  public void onEnabled(final ConfigurationContext context) {
    clientService = context.getProperty(CLIENT_SERVICE).asControllerService(ElasticSearchClientService.class);
    index = context.getProperty(INDEX).evaluateAttributeExpressions().getValue();
    type = context.getProperty(TYPE).evaluateAttributeExpressions().getValue();
    mapper = new ObjectMapper();

    final List<PropertyDescriptor> dynamicDescriptors = context.getProperties().keySet().stream()
            .filter(PropertyDescriptor::isDynamic)
            .collect(Collectors.toList());

    final Map<String, RecordPath> tempRecordPathMappings = new HashMap<>();
    for (final PropertyDescriptor desc : dynamicDescriptors) {
      final String value = context.getProperty(desc).getValue();
      final String name = desc.getName();
      tempRecordPathMappings.put(name, RecordPath.compile(value));
    }

    recordPathMappings = new ConcurrentHashMap<>(tempRecordPathMappings);

    super.onEnabled(context);
  }

  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return descriptors;
  }

  @Override
  public PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String name) {
    return new PropertyDescriptor.Builder()
            .name(name)
            .addValidator((subject, input, context) -> {
              ValidationResult.Builder builder = new ValidationResult.Builder();
              try {
                JsonPath.parse(input);
                builder.valid(true);
              } catch (Exception ex) {
                builder.explanation(ex.getMessage())
                        .valid(false)
                        .subject(subject);
              }

              return builder.build();
            })
            .dynamic(true)
            .build();
  }

  @Override
  public Optional<Record> lookup(final Map<String, Object> coordinates) throws LookupFailureException {
    final Map<String, String> context = coordinates.entrySet().stream()
            .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> e.getValue().toString()
            ));
    return lookup(coordinates, context);
  }

  @Override
  public Optional<Record> lookup(final Map<String, Object> coordinates, final Map<String, String> context) throws LookupFailureException {
    validateCoordinates(coordinates);

    try {
      final Record record;
      if (coordinates.containsKey("_id")) {
        record = getById((String) coordinates.get("_id"), context);
      } else {
        record = getByQuery(coordinates, context);
      }

      return record == null ? Optional.empty() : Optional.of(record);
    } catch (Exception ex) {
      getLogger().error("Error during lookup.", ex);
      throw new LookupFailureException(ex);
    }
  }

  private void validateCoordinates(final Map<String, Object> coordinates) throws LookupFailureException {
    final List<String> reasons = new ArrayList<>();

    if (coordinates.containsKey("_id") && !(coordinates.get("_id") instanceof String)) {
      reasons.add("_id was supplied, but it was not a String.");
    }

    if (coordinates.containsKey("_id") && coordinates.size() > 1) {
      reasons.add("When _id is used, it can be the only key used in the lookup.");
    }

    if (!reasons.isEmpty()) {
      final String error = String.join("\n", reasons);
      throw new LookupFailureException(error);
    }
  }

  @SuppressWarnings("unchecked")
  private Record getById(final String _id, final Map<String, String> context) throws IOException, LookupFailureException, SchemaNotFoundException {
    final Map<String, Object> query = new HashMap<String, Object>() {{
      put("query", new HashMap<String, Object>() {{
        put("match", new HashMap<String, String>() {{
          put("_id", _id);
        }});
      }});
    }};

    final String json = mapper.writeValueAsString(query);

    final SearchResponse response = clientService.search(json, index, type, null);
    if (response.getNumberOfHits() > 1) {
      throw new LookupFailureException(String.format("Expected 1 response, got %d for query %s",
              response.getNumberOfHits(), json));
    } else if (response.getNumberOfHits() == 0) {
      return null;
    }

    final Map<String, Object> source = (Map<String, Object>) response.getHits().get(0).get("_source");

    final RecordSchema toUse = getSchema(context, source, null);

    Record record = new MapRecord(toUse, source);
    if (recordPathMappings.size() > 0) {
      record = applyMappings(record, source);
    }

    return record;
  }

  Map<String, Object> getNested(final String key, final Object value) {
    final String path = key.substring(0, key.lastIndexOf("."));

    return new HashMap<String, Object>() {{
      put("path", path);
      put("query", new HashMap<String, Object>() {{
        put("match", new HashMap<String, Object>() {{
          put(key, value);
        }});
      }});
    }};
  }

  private Map<String, Object> buildQuery(final Map<String, Object> coordinates) {
    final Map<String, Object> query = new HashMap<String, Object>() {{
      put("bool", new HashMap<String, Object>() {{
        put("must", coordinates.entrySet().stream()
                .map(e -> new HashMap<String, Object>() {{
                  if (e.getKey().contains(".")) {
                    put("nested", getNested(e.getKey(), e.getValue()));
                  } else {
                    put("match", new HashMap<String, Object>() {{
                      put(e.getKey(), e.getValue());
                    }});
                  }
                }}).collect(Collectors.toList())
        );
      }});
    }};

    return new HashMap<String, Object>() {{
      put("size", 1);
      put("query", query);
    }};
  }

  @SuppressWarnings("unchecked")
  private Record getByQuery(final Map<String, Object> query, final Map<String, String> context) throws LookupFailureException {
    try {
      final String json = mapper.writeValueAsString(buildQuery(query));

      final SearchResponse response = clientService.search(json, index, type, null);
      if (response.getNumberOfHits() == 0) {
        return null;
      } else {
        final Map<String, Object> source = (Map<String, Object>) response.getHits().get(0).get("_source");
        final RecordSchema toUse = getSchema(context, source, null);
        Record record = new MapRecord(toUse, source);
        if (recordPathMappings.size() > 0) {
          record = applyMappings(record, source);
        }

        return record;
      }

    } catch (Exception e) {
      throw new LookupFailureException(e);
    }
  }

  private Record applyMappings(final Record record, final Map<String, Object> source) {
    final Record rec = new MapRecord(record.getSchema(), new HashMap<>());

    recordPathMappings.forEach((key, path) -> {
      try {
        final Object o = JsonPath.read(source, key);
        final Optional<FieldValue> first = path.evaluate(rec).getSelectedFields().findFirst();
        first.ifPresent(fieldValue -> fieldValue.updateValue(o));
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    });

    return rec;
  }

  @Override
  public Class<?> getValueType() {
    return Record.class;
  }

  @Override
  public Set<String> getRequiredKeys() {
    return Collections.emptySet();
  }
}
