package com.yoshio3;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.SqlParameter;
import com.azure.cosmos.models.SqlQuerySpec;
import com.azure.cosmos.util.CosmosPagedFlux;
import com.yoshio3.entities.CosmosDBDocument;

import jakarta.annotation.PostConstruct;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
public class CosmosDBUtil {

	private final Logger LOGGER = LoggerFactory.getLogger(CosmosDBUtil.class);

    @Value("${azure.cosmos.db.endpoint}")
    private String COSMOS_DB_ENDPOINT;

    @Value("${azure.cosmos.db.key}")
    private String COSMOS_DB_KEY;

    @Value("${azure.cosmos.db.database.name}")
    private String COSMOS_DB_DATABASE_NAME;

    @Value("${azure.cosmos.db.container.name}")
    private String COSMOS_DB_CONTAINER_NAME;

    private static final String RETRIVE_REGISTERED_DOCUMENTS_QUERY = "SELECT * FROM c WHERE c.status = 'COMPLETED' ORDER BY c.fileName ASC, c.pageNumber ASC";

    private static final String RETRIVE_FAILED_DOCUMENTS_QUERY = "SELECT * FROM c WHERE c.status != 'COMPLETED' ORDER BY c.fileName ASC, c.pageNumber ASC";

    private static final String ALL_FILE_NAME_QUERY = "SELECT c.fileName FROM c GROUP BY c.fileName";

    private CosmosAsyncContainer container = null;
    private CosmosAsyncClient client = null;

    @PostConstruct
    private void init() {
        client = new CosmosClientBuilder()
                .endpoint(COSMOS_DB_ENDPOINT)
                .key(COSMOS_DB_KEY)
                .buildAsyncClient();
        var database = client.getDatabase(COSMOS_DB_DATABASE_NAME);
        container = database.getContainer(COSMOS_DB_CONTAINER_NAME);
    }

    // DB に登録が成功しているドキュメントを取得
    public Mono<List<CosmosDBDocument>> getAllRegisteredDocuments() {
        CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();
        options.setQueryMetricsEnabled(true);
        SqlQuerySpec querySpec = new SqlQuerySpec(RETRIVE_REGISTERED_DOCUMENTS_QUERY);

        CosmosPagedFlux<CosmosDBDocument> queryItems = container.queryItems(querySpec, options, CosmosDBDocument.class);
        return queryItems.byPage().flatMap(page -> Flux.fromIterable(page.getResults())).collectList();
    }

    // DB の登録に失敗しているドキュメントを取得
    public Mono<List<CosmosDBDocument>> getAllFailedDocuments() {
        CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();
        options.setQueryMetricsEnabled(true);
        SqlQuerySpec querySpec = new SqlQuerySpec(RETRIVE_FAILED_DOCUMENTS_QUERY);

        CosmosPagedFlux<CosmosDBDocument> queryItems = container.queryItems(querySpec, options, CosmosDBDocument.class);
        return queryItems.byPage().flatMap(page -> Flux.fromIterable(page.getResults())).collectList();
    }

    // ファイル名の一覧を取得
    public List<String> getDocumentFileNames() throws InterruptedException {
        var options = new CosmosQueryRequestOptions();
        options.setQueryMetricsEnabled(true);
        var querySpec = new SqlQuerySpec(ALL_FILE_NAME_QUERY);

    	var executor = Executors.newSingleThreadExecutor();
    	var  cdl = new CountDownLatch(1);
    	var result = new AtomicReference<List<String>>();
    	executor.execute(() -> {
    		container.queryItems(querySpec, options, CosmosDBDocument.class).byPage().subscribe(res -> {
    			List<String> fileNames = res.getResults().stream().map(doc -> doc.fileName()).collect(Collectors.toList());
    			result.set(fileNames);
    		}, e -> {
    			LOGGER.error("Cosmos DB query Failed.", e);
    		}, () -> {
        		cdl.countDown();
    		});
    	});
    	cdl.await(60000, TimeUnit.MILLISECONDS);
        return result.get();
    }
    
    public void deleteDocuments(List<String> fileNames) throws InterruptedException {
    	if (fileNames == null || fileNames.isEmpty()) {
    		return;
    	}
        CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();
        options.setQueryMetricsEnabled(true);

    	var executor = Executors.newSingleThreadExecutor();

    	var selectCdl = new CountDownLatch(fileNames.size());
    	var selectResult = new AtomicReference<List<String>>(new ArrayList<String>());
    	executor.execute(() -> {
            for (var fileName : fileNames) {
        		var querySpec = new SqlQuerySpec("SELECT c.id FROM c WHERE c.fileName = @fileName", new SqlParameter("@fileName", fileName));
        		container.queryItems(querySpec, options, CosmosDBDocument.class).byPage().subscribe(res -> {
        			var results = res.getResults();
        			if (results != null && !results.isEmpty()) {
        				selectResult.updateAndGet(list -> {
        					list.addAll(results.stream().map(doc -> doc.id()).collect(Collectors.toList()));
        					return list;
        				});
        			}
        		}, e -> {
        			LOGGER.error("Cosmos DB query Failed.", e);
        		}, () -> {
        			selectCdl.countDown();
        		});
        	}
    	});
    	selectCdl.await(60000, TimeUnit.MILLISECONDS);
    	var deleteIds = selectResult.get();
    	if (deleteIds == null || deleteIds.isEmpty()) {
    		return;
    	}

    	var deleteCdl = new CountDownLatch(deleteIds.size());
    	executor.execute(() -> {
    		for (var deleteId : deleteIds) {
    			container.deleteItem(deleteId, new PartitionKey(deleteId)).subscribe(res -> {
    				var statusCode = res.getStatusCode();
    				LOGGER.info("delete documents[id={},statusCode={}]", deleteId, statusCode);
    			}, e -> {
        			LOGGER.error("Cosmos DB delete Failed.", e);
    			}, () -> {
    				deleteCdl.countDown();
    			});
    		}
    	});
    	deleteCdl.await(60000, TimeUnit.MILLISECONDS);
    }
}
