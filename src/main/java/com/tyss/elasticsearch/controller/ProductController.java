package com.tyss.elasticsearch.controller;

import com.tyss.elasticsearch.entity.Product;
import com.tyss.elasticsearch.service.ElasticSearchService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;

@RestController
@RequestMapping("/api/v1/products")
@Slf4j
public class ProductController {

    private ElasticSearchService elasticSearchService;

    public ProductController(ElasticSearchService elasticSearchService) {
        this.elasticSearchService = elasticSearchService;
    }


    @PostMapping
    public ResponseEntity<String> saveProduct(@RequestBody Product product) throws IOException {
        log.info("Controller Started");
        String product1 = elasticSearchService.saveProduct(product);
        return new ResponseEntity<>(product1, HttpStatus.CREATED);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Product> findProductById(@PathVariable String id) throws IOException {
        log.info("Controller Started");
        return new ResponseEntity<>(elasticSearchService.findProductById(id), HttpStatus.OK);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<String> deleteProduct(@PathVariable String id) throws IOException {
        log.info("Controller Started");
        String string = elasticSearchService.deleteProduct(id);
        return new ResponseEntity<>(string, HttpStatus.OK);
    }

    @PutMapping
    public ResponseEntity<String> updateProduct(@RequestBody Product product) throws IOException {
        log.info("Controller Started");
        String string = elasticSearchService.updateProduct(product);
        return new ResponseEntity<>(string, HttpStatus.OK);
    }

    @GetMapping
    public List<Product> findProductByName(@RequestParam String name) throws IOException {
        log.info("Controller Started");
        return elasticSearchService.findProductByName(name);
    }

    @GetMapping("/find/{quantity}")
    public List<Product> searchByQuantity(@PathVariable int quantity) throws IOException {
        log.info("Controller Started");
        return elasticSearchService.searchByMaxQuantity(quantity);

    }

    @GetMapping("/aggregation")
    public List<Product> findProductByquantityinterval(@RequestParam String name, @RequestParam double interval) throws IOException {
        log.info("Controller Started");
        return elasticSearchService.findProductByquantityinterval(name, interval);
    }

    @GetMapping("/findAll")
    public List<Product> findAllProduct() throws IOException {
        log.info("Controller Started");
        return elasticSearchService.findAllProduct();
    }

    @GetMapping("/match")
    public List<Product> findProductwithQuantityAndName(@RequestParam int quantity, @RequestParam String name) throws IOException {
        log.info("Controller Started");
        return elasticSearchService.findProductwithQuantityAndName(quantity, name);
    }

}
