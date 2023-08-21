package com.yoshio3;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.SqlParameter;
import com.azure.cosmos.models.SqlQuerySpec;
import com.yoshio3.logging.BDLogger;
import com.yoshio3.models.CosmosDBDocument;
import com.yoshio3.models.CosmosDBDocumentStatus;

public class CosmosDBUtil {

	private static final String COSMOS_DB_ENDPOINT;
	private static final String COSMOS_DB_KEY;
	private static final String COSMOS_DB_DATABASE_NAME;
	private static final String COSMOS_DB_CONTAINER_NAME;

	static {
		COSMOS_DB_ENDPOINT = System.getenv("AzureCosmosDbEndpoint");
		COSMOS_DB_KEY = System.getenv("AzureCosmosDbKey");
		COSMOS_DB_DATABASE_NAME = System.getenv("AzureCosmosDbDatabaseName");
		COSMOS_DB_CONTAINER_NAME = System.getenv("AzureCosmosDbContainerName");
	}

	private CosmosAsyncContainer container = null;
	private CosmosAsyncClient client = null;

	public CosmosDBUtil() {
		client = new CosmosClientBuilder()
				.endpoint(COSMOS_DB_ENDPOINT)
				.key(COSMOS_DB_KEY)
				.buildAsyncClient();
		var database = client.getDatabase(COSMOS_DB_DATABASE_NAME);
		container = database.getContainer(COSMOS_DB_CONTAINER_NAME);
	}

	public boolean isRegisteredDocument(String fileName, final BDLogger logger) throws InterruptedException {
		var executor = Executors.newSingleThreadExecutor();
		var cdl = new CountDownLatch(1);
		var result = new AtomicReference<Boolean>(false);
		executor.execute(() -> {
			CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();
			options.setQueryMetricsEnabled(true);
			var querySpec = new SqlQuerySpec(
					"SELECT c.fileName FROM c WHERE c.fileName = @fileName GROUP BY c.fileName",
					new SqlParameter("@fileName", fileName));
			container.queryItems(querySpec, options, CosmosDBDocument.class).byPage().subscribe(res -> {
				var results = res.getResults();
				if (results != null && !results.isEmpty()) {
					result.set(true);
				}
			}, e -> {
				logger.severe("Cosmos DB read Failed.", e);
			}, () -> {
				cdl.countDown();
			});
		});
		cdl.await(60000, TimeUnit.MILLISECONDS);
		return result.get();
	}

	public CosmosDBDocument createDocument(
			String id,
			String fileName,
			CosmosDBDocumentStatus status,
			int pageNumber,
			final BDLogger logger) throws InterruptedException {
		var executor = Executors.newSingleThreadExecutor();
		var cdl = new CountDownLatch(1);
		var result = new AtomicReference<CosmosDBDocument>();
		executor.execute(() -> {
			var document = new CosmosDBDocument(id, fileName, status, pageNumber);
			logger.info("Cosmos DB create Document: " + document);
			container.createItem(document, null).subscribe(res -> {
				result.set(res.getItem());
			}, e -> {
				logger.severe("Cosmos DB create Failed.", e);
			}, () -> {
				cdl.countDown();
			});
		});
		cdl.await(60000, TimeUnit.MILLISECONDS);
		return result.get();
	}

	public boolean updateStatus(
			String id,
			CosmosDBDocumentStatus status,
			final BDLogger logger) throws InterruptedException {
		var executor = Executors.newSingleThreadExecutor();
		logger.info("Cosmos DB update Status: Start " + id + ":" + status);

		var selectCdl = new CountDownLatch(1);
		var selectResult = new AtomicReference<CosmosDBDocument>();
		executor.execute(() -> {
			container.readItem(id, new PartitionKey(id), CosmosDBDocument.class).subscribe(res -> {
				CosmosDBDocument item = res.getItem();
				logger.info("Cosmos DB read Document: " + item);
				selectResult.set(new CosmosDBDocument(item.id(), item.fileName(), status, item.pageNumber()));
			}, e -> {
				logger.severe("Cosmos DB read Failed.", e);
			}, () -> {
				selectCdl.countDown();
			});
		});
		selectCdl.await(60000, TimeUnit.MILLISECONDS);
		var updateDocument = selectResult.get();
		if (updateDocument == null) {
			return false;
		}

		var updateCdl = new CountDownLatch(1);
		var updateResult = new AtomicReference<Boolean>(false);
		executor.execute(() -> {
			container.replaceItem(updateDocument, updateDocument.id(),
					new PartitionKey(updateDocument.id()), null).subscribe(res -> {
						logger.info("Cosmos DB update Status: " + id + ":" + updateDocument);
						logger.fine("Cosmos DB Update Response Code : " + res.getStatusCode());
						updateResult.set(true);
					}, e -> {
						logger.severe("Cosmos DB update Failed.", e);
					}, () -> {
						updateCdl.countDown();
						logger.fine("Cosmos DB update Completed");
					});
		});
		updateCdl.await(60000, TimeUnit.MILLISECONDS);
		return updateResult.get();
	}

}
