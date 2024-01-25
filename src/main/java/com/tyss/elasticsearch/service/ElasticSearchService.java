package com.tyss.elasticsearch.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import com.tyss.elasticsearch.entity.Product;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.stereotype.Service;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Service
@NoArgsConstructor
@Slf4j
public class ElasticSearchService {

    private ElasticsearchClient elasticsearchClient;

    @Autowired
    public ElasticSearchService(ElasticsearchClient elasticsearchClient) {
        this.elasticsearchClient = elasticsearchClient;
    }

    public String saveProduct(Product product) throws IOException {
        log.info("Service Started");
        String indexName = product.getClass().getAnnotation(Document.class).indexName();
        IndexResponse response = elasticsearchClient.index(i -> i.index(indexName).id(product.getId()).document(product));
        log.info("Data created" + response.toString());
        return response.toString() + " Created";
    }


    public Product findProductById(String id) throws IOException {
        log.info("Service Started");
        String indexName = Product.class.getAnnotation(Document.class).indexName();
        GetResponse<Product> response = elasticsearchClient.get(g -> g.index(indexName).id(id), Product.class);
        log.info("Data Recieved :" + response.source());
        return response.source();

    }

    public List<Product> findProductByName(String name) throws IOException {
        log.info("Service Started");
        String indexName = Product.class.getAnnotation(Document.class).indexName();
        SearchResponse<Product> response = elasticsearchClient.search(s -> s.index(indexName).query(q -> q.match(m -> m.field("name").query(name))), Product.class);

        List<Hit<Product>> hits = response.hits().hits();
        List<Product> products = new ArrayList<>();
        for (Hit<Product> hit : hits) {
            Product product = hit.source();
            products.add(product);
        }
        log.info("Data recieved" + products);
        return products;

    }

    public List<Product> searchByMaxQuantity(int quantity) throws IOException {
        log.info("Service Started");
        String indexName = Product.class.getAnnotation(Document.class).indexName();
        SearchResponse<Product> response = elasticsearchClient.search(s -> s.index(indexName).query(RangeQuery.of(r -> r.field("quantity").gte(JsonData.of(quantity)))._toQuery()), Product.class);
        List<Hit<Product>> hits = response.hits().hits();
        List<Product> products = new ArrayList<>();
        for (Hit<Product> hit : hits) {
            Product product = hit.source();
            products.add(product);
        }
        return products;
    }

    public List<Product> findProductByquantityinterval(String name, double interval) throws IOException {
        log.info("Service Started");
        Query query = MatchQuery.of(m -> m.field("name").query(name))._toQuery();
        String indexName = Product.class.getAnnotation(Document.class).indexName();

        SearchResponse<Product> response = elasticsearchClient.search(b -> b.index(indexName).size(10).query(query).aggregations("quantity-histogram", a -> a.histogram(h -> h.field("quantity").interval(interval))), Product.class);
        List<Hit<Product>> hits = response.hits().hits();
        List<Product> products = new ArrayList<>();
        for (Hit<Product> hit : hits) {
            Product product = hit.source();
            products.add(product);
        }
        return products;
    }

    public String updateProduct(Product product) throws IOException {
        log.info("Service Started");
        String indexName = Product.class.getAnnotation(Document.class).indexName();
        UpdateResponse<Product> update = elasticsearchClient.update(s -> s.index(indexName).id(product.getId()).doc(product), Product.class);
        return update.result().toString();
    }

    public String deleteProduct(String id) throws IOException {
        log.info("Service Started");
        String indexName = Product.class.getAnnotation(Document.class).indexName();
        DeleteResponse delete = elasticsearchClient.delete(d -> d.index(indexName).id(id));
        return delete.result().toString();
    }

    public List<Product> findAllProduct() throws IOException {
        log.info("Service Started");
        String indexName = Product.class.getAnnotation(Document.class).indexName();
        SearchResponse<Product> response = elasticsearchClient.search(s -> s.index(indexName).size(100), Product.class);
        List<Product> collect = response.hits().hits().stream().map(Hit::source).collect(Collectors.toList());
        for (Product product : collect) {
            log.info(" values are :" + product);
        }

        return collect;
    }

    public List<Product> findProductwithQuantityAndName(int quantity, String name) throws IOException {
        log.info("Service Started");
        String indexName = Product.class.getAnnotation(Document.class).indexName();
        Query query = MatchQuery
                .of(m -> m.field("name")
                        .query(name))
                ._toQuery();

        Query query1 = RangeQuery
                .of(r -> r.field("quantity")
                        .gte(JsonData.of(quantity)))
                ._toQuery();

        SearchResponse<Product> response = elasticsearchClient
                .search(s -> s.index(indexName)
                        .query(q -> q.bool(b -> b.must(query)
                                .must(query1))), Product.class);

        return response.hits().hits().stream().map(Hit::source).collect(Collectors.toList());
    }

}





