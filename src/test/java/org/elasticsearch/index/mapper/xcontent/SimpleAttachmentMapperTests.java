/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper.xcontent;

import org.apache.lucene.document.Document;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.attachment.AttachmentMapper;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.elasticsearch.common.io.Streams.copyToBytesFromClasspath;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
@Test
public class SimpleAttachmentMapperTests {

    private DocumentMapperParser mapperParser;

    @BeforeClass
    public void setupMapperParser() {
        mapperParser = new DocumentMapperParser(new Index("test"), new AnalysisService(new Index("test")));
        mapperParser.putTypeParser(AttachmentMapper.CONTENT_TYPE, new AttachmentMapper.TypeParser());
    }

    @Test
    public void testSimpleMappings() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/xcontent/test-mapping.json");
        DocumentMapper docMapper = mapperParser.parse(mapping);
        byte[] html = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/xcontent/testXHTML.html");

        BytesReference json = jsonBuilder().startObject().field("_id", 1).field("file", html).endObject().bytes();

        Document doc = docMapper.parse(json).rootDoc();

        assertThat(doc.get(docMapper.mappers().smartName("file.content_type").mapper().names().indexName()), equalTo("application/xhtml+xml"));
        assertThat(doc.get(docMapper.mappers().smartName("file.title").mapper().names().indexName()), equalTo("XHTML test document"));
        assertThat(doc.get(docMapper.mappers().smartName("file").mapper().names().indexName()), containsString("This document tests the ability of Apache Tika to extract content"));

        // re-parse it
        String builtMapping = docMapper.mappingSource().string();
        docMapper = mapperParser.parse(builtMapping);

        json = jsonBuilder().startObject().field("_id", 1).field("file", html).endObject().bytes();

        doc = docMapper.parse(json).rootDoc();

        assertThat(doc.get(docMapper.mappers().smartName("file.content_type").mapper().names().indexName()), equalTo("application/xhtml+xml"));
        assertThat(doc.get(docMapper.mappers().smartName("file.title").mapper().names().indexName()), equalTo("XHTML test document"));
        assertThat(doc.get(docMapper.mappers().smartName("file").mapper().names().indexName()), containsString("This document tests the ability of Apache Tika to extract content"));
    }
}
