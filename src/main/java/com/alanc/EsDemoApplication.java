package com.alanc;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@SpringBootApplication
@RestController
public class EsDemoApplication {

	@Autowired
	private TransportClient transportClient;

	@GetMapping("/")
	public String index(){
		return "index";
	}

	@GetMapping(value = "/get/book/novel")
	@ResponseBody
	public ResponseEntity getBook(@RequestParam(name = "id", defaultValue = "")String id){

		if (id.isEmpty()){
			return new ResponseEntity(HttpStatus.NOT_FOUND);
		}
		GetResponse result = this.transportClient.prepareGet("book", "novel", id).get();
		if (!result.isExists()){
			return new ResponseEntity(HttpStatus.NOT_FOUND);
		}

		return new ResponseEntity(result.getSource(), HttpStatus.OK);
	}

	@PostMapping("/add/book/novel")
	@ResponseBody
	public ResponseEntity addBook(@RequestParam(name = "title")String title,
								  @RequestParam(name = "author") String author,
								  @RequestParam(name = "word_count")int wordCount,
								  @RequestParam(name = "publish_date") @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") Date publishDate){

		try {
			XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().field("title", title)
					.field("author", author)
					.field("word_count", wordCount)
					.field("publish_date", publishDate.getTime()).endObject();

			IndexResponse result = this.transportClient.prepareIndex("book", "novel").setSource(xContentBuilder).get();

			return new ResponseEntity(result.getId(), HttpStatus.OK);
		} catch (IOException e) {
			e.printStackTrace();
			return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	@PostMapping("/update/book/novel")
	@ResponseBody
	public ResponseEntity updateBook(@RequestParam(name = "id")String id,
									 @RequestParam(name = "title", required = false)String title,
                                     @RequestParam(name = "author", required = false) String author,
                                     @RequestParam(name = "word_count", required = false) Integer word_count){
        UpdateRequest requestUpdate = new UpdateRequest("book", "novel", id);

        try {
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject();

            if (title != null){
                xContentBuilder.field("title", title);
            }
            if (author != null){
                xContentBuilder.field("author", author);
            }
            //springmvc在接受参数时不存在会转为null，int无法接受，要么用包装类型，要么给个默认值
            if (word_count != null){
                System.out.println("word_count: " + word_count);
                xContentBuilder.field("word_count", word_count);
            }
            xContentBuilder.endObject();
            requestUpdate.doc(xContentBuilder);
        } catch (IOException e) {
            e.printStackTrace();
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        try {
            UpdateResponse updateResponse = this.transportClient.update(requestUpdate).get();

            return new ResponseEntity(updateResponse.getResult().toString(), HttpStatus.OK);
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }

    }

	@PostMapping("/delete/book/novel")
	public ResponseEntity deleteBook(@RequestParam(name = "id") String id){
		if (id.isEmpty()){
			return new ResponseEntity(HttpStatus.NOT_FOUND);
		}
		DeleteResponse result = this.transportClient.prepareDelete("book", "novel", id).get();
		return new ResponseEntity(result.getResult().toString(), HttpStatus.OK);
	}

	@PostMapping("/query/book/novel")
    @ResponseBody
	public ResponseEntity queryOption(@RequestParam(name = "title", required = false)String title,
                                      @RequestParam(name = "author", required = false) String author,
                                      @RequestParam(name = "gt_word_count", defaultValue = "0") int gtWordCount,
                                      @RequestParam(name = "lt_word_count", required = false) Integer ltWordCount){
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        if (title != null){
            boolQueryBuilder.must(QueryBuilders.matchQuery("title", title));
        }
        if (author != null){
            boolQueryBuilder.must(QueryBuilders.matchQuery("author", author));
        }
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("word_count").from(gtWordCount);
        if (ltWordCount != null && ltWordCount >0){
            rangeQueryBuilder.to(ltWordCount);
        }
        boolQueryBuilder.filter(rangeQueryBuilder);
        SearchRequestBuilder searchRequestBuilder = this.transportClient.prepareSearch("book").setTypes("novel")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolQueryBuilder).setFrom(0).setSize(10);

        System.out.println(searchRequestBuilder);

        SearchResponse searchResponse = searchRequestBuilder.get();

        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
//        for (SearchHit hit : searchResponse.getHits()) {
//            result.add(hit.getSourceAsMap());
//
//        }
        searchResponse.getHits().forEach(((hit) -> result.add(hit.getSourceAsMap())));
        return new ResponseEntity(result, HttpStatus.OK);
    }

	public static void main(String[] args) {
		SpringApplication.run(EsDemoApplication.class, args);
	}


}
