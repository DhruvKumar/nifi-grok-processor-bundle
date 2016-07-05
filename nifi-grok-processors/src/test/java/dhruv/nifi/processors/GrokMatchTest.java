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

import java.io.InputStream;
import java.util.function.Consumer;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class GrokMatchTest {

    private static final String PATTERN = "%{SYSLOGTIMESTAMP:date} %{USERNAME:username} %{GREEDYDATA:data}";
    private static final String PATTERN_KEY = "username";

    private static final String matched_log = "/grok_matched.log";

    private static final String unmatched_log = "/grok_unmatched.log";

    private static final String TEST_MATCH_VALUE = "test";

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(GrokMatch.class);
    }

    @Test
    public void testMatchedResults() {
        testRunner.setProperty(GrokMatch.GROK_PATTERN_PROPERTY, PATTERN);
        InputStream resourceAsStream = GrokMatch.class.getResourceAsStream(matched_log);
        testRunner.enqueue(resourceAsStream);

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(GrokMatch.MATCHED_RELATIONSHIP);
        testRunner.assertTransferCount(GrokMatch.MATCHED_RELATIONSHIP, 1);
        testRunner.getFlowFilesForRelationship(GrokMatch.MATCHED_RELATIONSHIP).forEach(new Consumer<MockFlowFile>() {
            @Override
            public void accept(MockFlowFile t) {
                t.assertAttributeEquals(PATTERN_KEY, TEST_MATCH_VALUE);
            }
        });
    }

    @Test
    public void testUnMatchedResults() {
        testRunner.setProperty(GrokMatch.GROK_PATTERN_PROPERTY, PATTERN);
        InputStream resourceAsStream = GrokMatch.class.getResourceAsStream(unmatched_log);
        testRunner.enqueue(resourceAsStream);
        testRunner.assertAllFlowFilesTransferred(GrokMatch.UNMATCHED_RELATIONSHIP);
    }

}
