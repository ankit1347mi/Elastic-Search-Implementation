package com.tyss.elasticsearch.util;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class ElasticSearchUtil {
    String serverUrl = "http://localhost:9200";

    @Bean
    public RestClient restClient() {
        return RestClient.builder(HttpHost.create(serverUrl)).build();
    }

    @Bean
    public RestClientTransport restClientTransport(RestClient restClient) {
        return new RestClientTransport(restClient, new JacksonJsonpMapper());
    }

    @Bean
    public ElasticsearchClient elasticSearchClient(RestClientTransport restClientTransport) {
        return new ElasticsearchClient(restClientTransport);
    }
}
