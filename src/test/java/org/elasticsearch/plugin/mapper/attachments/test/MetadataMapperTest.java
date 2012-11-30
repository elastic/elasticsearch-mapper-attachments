package org.elasticsearch.plugin.mapper.attachments.test;

import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.testng.annotations.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.elasticsearch.client.Requests.putMappingRequest;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.testng.Assert.assertEquals;

/**
 * @author fcamblor
 * Test designed to demonstrate and fix https://github.com/elasticsearch/elasticsearch-mapper-attachments/issues/15
 */
public class MetadataMapperTest {

    private static final String INDEX_NAME = "testing";
    private static final String CONTENT_NAME = "test";
    private static final String RAW_FILE_CONTENT_FIELDNAME = "rawContent";

    private Node node;

    @BeforeClass
    public void setupServer() {
        node = nodeBuilder().local(true).settings(settingsBuilder()
                .put("path.data", "target/data")
                .put("cluster.name", "test-cluster-" + NetworkUtils.getLocalAddress())
                .put("gateway.type", "none")).node();
    }

    @AfterClass
    public void closeServer() {
        node.close();
    }

    @BeforeMethod
    public void setup() throws IOException {
        createIndex();
    }

    @AfterMethod
    public void teardown(){
        deleteIndex();
    }

    @Test
    public void shouldHtmlFileWithoutDateMetaBeIndexedThenFetchedCorrectly() throws Throwable {
        indexContentOf("htmlWithoutDateMeta.html");

        // Seems like it is needed for indexes having the time to be created/propagated...
        Thread.sleep(1000);

        assertEquals(findDocuments("Hello").totalHits(), 1L);
        assertEquals(findDocuments("World").totalHits(), 1L);
        assertEquals(findDocuments("Blah").totalHits(), 0L);
    }

    @Test
    public void shouldHtmlFileWithEmptyDateMetaBeIndexedThenFetchedCorrectly() throws Throwable {
        indexContentOf("htmlWithEmptyDateMeta.html");

        // Seems like it is needed for indexes having the time to be created/propagated...
        Thread.sleep(1000);

        assertEquals(findDocuments("Hello").totalHits(), 1L);
        assertEquals(findDocuments("World").totalHits(), 1L);
        assertEquals(findDocuments("Blah").totalHits(), 0L);
    }

    @Test
    public void shouldHtmlFileWithValidDateMetaBeIndexedThenFetchedCorrectly() throws Throwable {
        indexContentOf("htmlWithValidDateMeta.html");

        // Seems like it is needed for indexes having the time to be created/propagated...
        Thread.sleep(1000);

        assertEquals(findDocuments("Hello").totalHits(), 1L);
        assertEquals(findDocuments("World").totalHits(), 1L);
        assertEquals(findDocuments("Blah").totalHits(), 0L);
    }

    protected SearchHits findDocuments(String... contentTerms){
        DisMaxQueryBuilder subquery = QueryBuilders.disMaxQuery();
        for(String contentTerm : contentTerms){
            // We're applying a lowercase filter, so, let's apply lowercase to search terms too
            String loweredCasedTerm = contentTerm.toLowerCase();

            subquery = subquery
                    .add(termQuery(RAW_FILE_CONTENT_FIELDNAME, loweredCasedTerm).boost(1))
                    .add(termQuery(RAW_FILE_CONTENT_FIELDNAME+".title", loweredCasedTerm).boost(10));
        }
        return findDocuments(boolQuery().must(subquery));
    }

    protected SearchHits findDocuments(QueryBuilder queryBuilder){
        return node.client().prepareSearch(INDEX_NAME)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(queryBuilder)
                .addSort("_score", SortOrder.DESC)
                .execute().actionGet().getHits();
    }

    protected void indexContentOf(String filename) throws URISyntaxException, IOException {
        URL resource = MetadataMapperTest.class.getClassLoader().getResource("org/elasticsearch/plugin/mapper/attachments/test/" + filename);
        Path path = Paths.get(resource.toURI());

        node.client().prepareIndex(INDEX_NAME, CONTENT_NAME, filename).setSource(
            jsonBuilder().startObject()
                .startObject(RAW_FILE_CONTENT_FIELDNAME)
                    .field("_name", path.getFileName().toString())
                    .field("_content_type", "text/html")
                    .field("content", Files.readAllBytes(path))
                .endObject()
            .endObject()
        ).execute().actionGet();
    }

    protected void createIndex() throws IOException {
        // Preparing index settings
        node.client().admin().indices().prepareCreate(INDEX_NAME)
            .setSettings(ImmutableSettings.settingsBuilder().loadFromSource(
                    jsonBuilder().startObject()
                        .startObject("analysis")
                            .startObject("analyzer")
                                // Thanks David Pilato https://gist.github.com/2146038
                                .startObject("francais")
                                    .field("type", "custom")
                                    .field("tokenizer", "standard")
                                    .startArray("filter")
                                        .value("lowercase")
                                        .value("stop_francais")
                                        .value("fr_stemmer")
                                        .value("asciifolding")
                                        .value("elision")
                                    .endArray()
                                .endObject()
                            .endObject()
                            .startObject("filter")
                                .startObject("stop_francais")
                                    .field("type", "stop")
                                    .startArray("stopwords").value("_french_").endArray()
                                .endObject()
                                .startObject("fr_stemmer")
                                    .field("type", "stemmer")
                                    .field("name", "french")
                                .endObject()
                                .startObject("elision")
                                    .field("type", "elision")
                                    .startArray("articles")
                                        .value("l").value("m").value("t").value("qu")
                                        .value("n").value("s").value("j").value("d")
                                    .endArray()
                                .endObject()
                        .endObject()
                    .endObject().string()
            )).execute().actionGet();

        // Preparing index metadata for document
        node.client().admin().indices().putMapping(
                putMappingRequest(INDEX_NAME).type(CONTENT_NAME).source(
                        jsonBuilder().startObject()
                                .startObject(CONTENT_NAME)
                                .startObject("_all")
                                .field("analyzer", "francais")
                                .endObject()
                                .startObject("properties")
                                .startObject(RAW_FILE_CONTENT_FIELDNAME)
                                .field("type", "attachment")
                        /*
                                    .startObject("fields")
                                        .startObject("date").field("format", (String)null).endObject()
                                    .endObject()
                                    */
                                .endObject()
                                .endObject()
                                .endObject()
                                .string()
                )).actionGet();
    }

    protected void deleteIndex(){
        node.client().admin().indices().prepareDelete(INDEX_NAME).execute().actionGet();
    }
}
