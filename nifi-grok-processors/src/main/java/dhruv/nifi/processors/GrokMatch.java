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
package dhruv.nifi.processors;

import oi.thekraken.grok.api.Grok;
import oi.thekraken.grok.api.Match;
import oi.thekraken.grok.api.exception.GrokException;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class GrokMatch extends AbstractProcessor {

  public static final PropertyDescriptor GROK_PATTERN_PROPERTY = new PropertyDescriptor
      .Builder().name("GrokMatch Pattern")
      .description("GrokMatch pattern to match on")
      .required(true)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .build();

  public static final PropertyDescriptor MAX_BUFFER_SIZE = new PropertyDescriptor.Builder()
      .name("Maximum Buffer Size")
      .description("Specifies the maximum amount of data to buffer (per file) in order to apply the regular expressions.  Files larger than the specified maximum will not be fully evaluated.")
      .required(true)
      .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
      .addValidator(StandardValidators.createDataSizeBoundsValidator(0, Integer.MAX_VALUE))
      .defaultValue("1 MB")
      .build();

  public static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
      .name("Character Set")
      .description("The Character Set in which the file is encoded")
      .required(true)
      .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
      .defaultValue("UTF-8")
      .build();

  public static final Relationship MATCHED_RELATIONSHIP = new Relationship.Builder()
      .name("match")
      .description("Flow files matched successfully will be routed to this relationship")
      .build();

  public static final Relationship UNMATCHED_RELATIONSHIP = new Relationship.Builder()
      .name("no match")
      .description("Flow files not matched will be routed to this relationship")
      .build();

  private List<PropertyDescriptor> descriptors;

  private Set<Relationship> relationships;

  volatile String patternToMatch;
  volatile oi.thekraken.grok.api.Grok grok;

  private final BlockingQueue<byte[]> bufferQueue = new LinkedBlockingQueue<>();

  final ProcessorLog logger = getLogger();

  @Override
  protected void init(final ProcessorInitializationContext context) {
    final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
    descriptors.add(GROK_PATTERN_PROPERTY);
    descriptors.add(MAX_BUFFER_SIZE);
    descriptors.add(CHARACTER_SET);
    this.descriptors = Collections.unmodifiableList(descriptors);

    final Set<Relationship> relationships = new HashSet<Relationship>();
    relationships.add(MATCHED_RELATIONSHIP);
    relationships.add(UNMATCHED_RELATIONSHIP);
    this.relationships = Collections.unmodifiableSet(relationships);

    grok = new oi.thekraken.grok.api.Grok();
    try {
      grok.addPatternFromFile("resources/grok-patterns.txt");
    } catch (GrokException e) {
      e.printStackTrace();
    }


  }

  @Override
  public Set<Relationship> getRelationships() {
    return this.relationships;
  }

  @Override
  public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return descriptors;
  }

  @OnScheduled
  public void onScheduled(final ProcessContext context) {


    patternToMatch = context.getProperty(GROK_PATTERN_PROPERTY).getValue();

    try {
      grok.compile(patternToMatch);
    } catch (GrokException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

    FlowFile flowFile = session.get();
    if (flowFile == null) {
      return;
    }

    final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());

    final String contentString;
    byte[] buffer = bufferQueue.poll();
    if (buffer == null) {
      final int maxBufferSize = context.getProperty(MAX_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
      buffer = new byte[maxBufferSize];
    }

    try {
      final byte[] byteBuffer = buffer;
      session.read(flowFile, new InputStreamCallback() {
        @Override
        public void process(InputStream in) throws IOException {
          StreamUtils.fillBuffer(in, byteBuffer, false);
        }
      });

      final long len = Math.min(byteBuffer.length, flowFile.getSize());
      contentString = new String(byteBuffer, 0, (int) len, charset);
    } finally {
      bufferQueue.offer(buffer);
    }

    final Match match = grok.match(contentString);
    match.captures();
    logger.info("Match pattern = " + patternToMatch);

    final Map<String, Object> matchMap = match.toMap();

    for (Map.Entry<String, Object> e : matchMap.entrySet()) {
      logger.info("key = {}, value = {}",new Object[] {e.getKey(), e.getValue()});
    }

    if (!matchMap.isEmpty()) {
      for (Map.Entry<String, Object> e : matchMap.entrySet()) {
        flowFile = session.putAttribute(flowFile, e.getKey(), e.getValue().toString());
      }
      session.getProvenanceReporter().modifyAttributes(flowFile);
      session.transfer(flowFile, MATCHED_RELATIONSHIP);
      logger.info("Matched grok pattern and added attributes");
    } else {
      session.transfer(flowFile, UNMATCHED_RELATIONSHIP);
      logger.info("Could not match pattern");

    }

  }
}
