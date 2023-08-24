package com.yoshio3;

import static com.yoshio3.utils.Throwing.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

import com.azure.ai.openai.OpenAIClient;
import com.azure.ai.openai.OpenAIClientBuilder;
import com.azure.ai.openai.models.EmbeddingsOptions;
import com.azure.core.credential.AzureKeyCredential;
import com.documents4j.api.DocumentType;
import com.documents4j.conversion.msoffice.MicrosoftPowerpointBridge;
import com.documents4j.job.LocalConverter;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.OutputBinding;
import com.microsoft.azure.functions.annotation.BindingName;
import com.microsoft.azure.functions.annotation.BlobInput;
import com.microsoft.azure.functions.annotation.BlobOutput;
import com.microsoft.azure.functions.annotation.BlobTrigger;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.StorageAccount;
import com.yoshio3.logging.BDLogger;
import com.yoshio3.logging.LogContainer;
import com.yoshio3.models.CosmosDBDocumentStatus;

public class Function {

	// Azure OpenAI の API キー
	private static final String OPENAI_API_KEY;
	// Azure OpenAI のインスタンスの URL
	private static final String OPENAI_URL;
	// Azure OpenAI のEmbeddingのモデル名
	private static final String OPENAI_EMBEDDING_MODEL_NAME;

	// Azure PostgreSQL の JDBC URL
	private static final String POSTGRESQL_JDBC_URL;
	// Azure PostgreSQL のユーザー名
	private static final String POSTGRESQL_USER;
	// Azure PostgreSQL のパスワード
	private static final String POSTGRESQL_PASSWORD;
	// Azure PostgreSQL のテーブル名
	private static final String POSTGRESQL_TABLE_NAME;
	// １ページに含まれる文字数の上限（これを超える場合はページを分割して処理する）
	private static final int MAX_SEPARATE_TOKEN_LENGTH = 7500;

	// Azure OpenAI のクライアント・インスタンス
	private OpenAIClient client;
	// Azure OpenAI の呼び出しリトライ回数
	private static final int MAX_OPENAI_INVOCATION_RETRY_COUNT = 3;

	// Azure Cosmos DB のクライアント・インスタンス
	CosmosDBUtil cosmosDBUtil;

	static {
		OPENAI_API_KEY = System.getenv("AzureOpenaiApiKey");
		OPENAI_URL = System.getenv("AzureOpenaiUrl");
		OPENAI_EMBEDDING_MODEL_NAME = System.getenv("AzureOpenaiEmbeddingModelName");

		POSTGRESQL_JDBC_URL = System.getenv("AzurePostgresqlJdbcurl");
		POSTGRESQL_USER = System.getenv("AzurePostgresqlUser");
		POSTGRESQL_PASSWORD = System.getenv("AzurePostgresqlPassword");
		POSTGRESQL_TABLE_NAME = System.getenv("AzurePostgresqlDbTableName");
	}
	
	private static File GetConvertTempDir() throws IOException {
		var tempDir = Paths.get(System.getProperty("user.dir"), "temp");
		if (Files.notExists(tempDir)) {
			Files.createDirectory(tempDir);
		}
		return tempDir.toFile();
	}

	public Function() {
		client = new OpenAIClientBuilder()
				.credential(new AzureKeyCredential(OPENAI_API_KEY))
				.endpoint(OPENAI_URL)
				.buildClient();
		cosmosDBUtil = new CosmosDBUtil();
	}

	// 注意：applications.properties で "azure.blobstorage.container.name=pdfs" を変更した場合は
	// @BlobTrigger, @BlobInput の path も変更する必要があります。 デフォルト値：(pdfs/{name})
	// 理由は、path で指定できる値は、constants で定義されているものだけで、プロパティから取得することはできないためです。
	@FunctionName("ProcessUploadedFile")
	@StorageAccount("AzureWebJobsStorage")
	public void run(
			@BlobTrigger(name = "content", path = "docs/{blobName}.{ext}", dataType = "binary") byte[] content,
			@BindingName("blobName") String fileBaseName,
			@BindingName("ext") String fileExt,
			@BlobInput(name = "inputBlob", path = "docs/{blobName}.{ext}", dataType = "binary") byte[] inputBlob,
			@BlobOutput(name = "outputFileContent", path = "docs/{blobName}.{ext}.pdf", dataType = "binary") OutputBinding<byte[]> outputFileContent,
			final ExecutionContext context) throws UnsupportedEncodingException {
		var logContainer = LogContainer.create(context);
		var fileName = new StringBuilder()
				.append(fileBaseName)
				.append(".")
				.append(fileExt)
				.toString();
		String encodedFileName = URLEncoder.encode(fileName, "UTF-8");
		logContainer.funcLogger().info("Function [ProcessUploadedFile] trigger file: " + encodedFileName);
		if ("pdf".equals(fileExt)) {
			analyzePdf(content, fileName, logContainer);
		} else if ("doc".equals(fileExt) || "docx".equals(fileExt)) {
			convertPdfAndUpdateStorage(content, fileName, outputFileContent, DocumentType.MS_WORD, logContainer);
		} else if ("xls".equals(fileExt) || "xlsx".equals(fileExt)) {
			convertPdfAndUpdateStorage(content, fileName, outputFileContent, DocumentType.MS_EXCEL, logContainer);
		} else if ("ppt".equals(fileExt) || "pptx".equals(fileExt)) {
			convertPdfAndUpdateStorage(content, fileName, outputFileContent, DocumentType.MS_POWERPOINT, logContainer);
		}
	}
	
	private void convertPdfAndUpdateStorage(
			byte[] content,
			String fileName,
			OutputBinding<byte[]> outputFileContent,
			DocumentType inputDocType,
			final LogContainer logContainer) {
		try {
			var builder = LocalConverter
					.builder()
	        		.baseFolder(GetConvertTempDir())
	    			.workerPool(20, 25, 2, TimeUnit.SECONDS)
	    			.processTimeout(5, TimeUnit.SECONDS);
			var converter = (DocumentType.MS_POWERPOINT.equals(inputDocType)) ?
					builder.enable(MicrosoftPowerpointBridge.class).build() : builder.build();
			try (var bais = new ByteArrayInputStream(content);
					var baos = new ByteArrayOutputStream()) {
				logContainer.funcLogger().info("Convert " + inputDocType + " to PDF start.");
				var conversion = converter
						.convert(bais)
						.as(inputDocType)
						.to(baos)
						.as(DocumentType.PDF)
						.schedule();
				var result = conversion.get();
				logContainer.funcLogger().info("Convert " + inputDocType + " to PDF end. [result=" + result + "]");
				if (!result) {
					return;
				}
				outputFileContent.setValue(baos.toByteArray());
			} finally {
				converter.shutDown();
			}
		} catch (Exception e) {
			logContainer.funcLogger().severe("Error convert PDF.", e);
		}
	}

	private void analyzePdf(byte[] content, String fileName, final LogContainer logContainer) {
		try {
			if (cosmosDBUtil.isRegisteredDocument(fileName, logContainer.cosmosLogger())) {
				logContainer.funcLogger().info("Already registered file: " + fileName);
				return;
			}
			var pageInfos = extractPDFtoTextByPage(logContainer.funcLogger(), content);
			try (var connection = DriverManager.getConnection(
					POSTGRESQL_JDBC_URL, POSTGRESQL_USER, POSTGRESQL_PASSWORD)) {
				connection.setAutoCommit(false);
				pageInfos.forEach(rethrow(pageInfo -> {
					insertDataToPostgreSQL(
							logContainer,
							connection,
							pageInfo.text(),
							fileName,
							pageInfo.pageNumber());
				}));
			} catch (Exception e) {
				logContainer.funcLogger().severe("Error trigger PDF.", e);
			}
		} catch (Exception e) {
			logContainer.funcLogger().severe("Error trigger PDF.", e);
		}
	}

	private List<PageInfo> extractPDFtoTextByPage(final BDLogger logger, byte[] content) {
		var allPages = new ArrayList<PageInfo>();
		try (var document = PDDocument.load(content)) {
			var textStripper = new PDFTextStripper();
			var numberOfPages = document.getNumberOfPages();

			// PDF ファイルのページ数分ループ
			IntStream.rangeClosed(1, numberOfPages).forEach(pageNumber -> {
				try {
					textStripper.setStartPage(pageNumber);
					textStripper.setEndPage(pageNumber);
					var pageText = textStripper.getText(document);

					// 改行コードを空白文字に置き換え
					pageText = pageText.replace("\n", " ");
					pageText = pageText.replaceAll("\\s{2,}", " ");

					// 1 ページのテキストが 7500 文字を超える場合は分割する
					if (pageText.length() > MAX_SEPARATE_TOKEN_LENGTH) {
						logger.fine("Split text: " + pageText.length());
						List<String> splitText = splitText(pageText, MAX_SEPARATE_TOKEN_LENGTH);
						splitText.forEach(text -> {
							var pageInfo = new PageInfo(pageNumber, text);
							allPages.add(pageInfo);
						});
					} else {
						var pageInfo = new PageInfo(pageNumber, pageText);
						allPages.add(pageInfo);
					}
				} catch (IOException e) {
					logger.severe("Error while extracting text from PDF.", e);
				}
			});
		} catch (IOException e) {
			logger.severe("Error while extracting text from PDF.", e);
		}
		return allPages;
	}

	// PostgreSQL に Vector データを挿入するサンプル
	private void insertDataToPostgreSQL(
			final LogContainer logContainer,
			Connection connection,
			String originText,
			String fileName,
			int pageNumber) throws InterruptedException {
		UUID uuid = UUID.randomUUID();
		String uuidString = uuid.toString();
		try {
			cosmosDBUtil.createDocument(uuidString, fileName,
					CosmosDBDocumentStatus.PAGE_SEPARATE_FINISHED, pageNumber, logContainer.cosmosLogger());

			// OpenAI Text Embedding を呼び出しベクター配列を取得
			List<Double> embedding = invokeTextEmbedding(uuidString, originText, logContainer);
			cosmosDBUtil.updateStatus(uuidString,
					CosmosDBDocumentStatus.FINISH_OAI_INVOCATION, logContainer.cosmosLogger());

			// ベクター配列を PostgreSQL に挿入
			var insertSql = "INSERT INTO " + POSTGRESQL_TABLE_NAME
					+ " (id, embedding, origntext, fileName, pageNumber) VALUES (?, ?::vector, ?, ?, ?)";
			try (var insertStatement = connection.prepareStatement(insertSql)) {
				insertStatement.setObject(1, uuid);
				insertStatement.setArray(2, connection.createArrayOf("double", embedding.toArray()));
				insertStatement.setString(3, originText);
				insertStatement.setString(4, fileName);
				insertStatement.setInt(5, pageNumber);
				insertStatement.executeUpdate();
				connection.commit();
				cosmosDBUtil.updateStatus(uuidString,
						CosmosDBDocumentStatus.FINISH_DB_INSERTION, logContainer.cosmosLogger());
			}
		} catch (Exception e) {
			logContainer.funcLogger().severe("Error while inserting data to PostgreSQL.", e);
			cosmosDBUtil.updateStatus(uuidString,
					CosmosDBDocumentStatus.FAILED_DB_INSERTION, logContainer.cosmosLogger());
		}
		cosmosDBUtil.updateStatus(uuidString, CosmosDBDocumentStatus.COMPLETED, logContainer.cosmosLogger());
	}

	/**
	 * テキスト・エンべディングの検証サンプル
	 * @throws InterruptedException 
	 */
	private List<Double> invokeTextEmbedding(
			String uuid,
			String originalText,
			final LogContainer logContainer) throws InterruptedException {
		List<Double> embedding = new ArrayList<>();
		var embeddingsOptions = new EmbeddingsOptions(Arrays.asList(originalText));

		int retryCount = 0;
		while (retryCount < MAX_OPENAI_INVOCATION_RETRY_COUNT) {
			try {
				// OpenAI API を呼び出し
				var result = client.getEmbeddings(OPENAI_EMBEDDING_MODEL_NAME, embeddingsOptions);
				// 利用状況を取得（使用したトークン数）
				var usage = result.getUsage();
				logContainer.funcLogger().info("Number of Prompt Token: " + usage.getPromptTokens()
						+ "Number of Total Token: " + usage.getTotalTokens());
				// ベクター配列を取得
				var findFirst = result.getData().stream().findFirst();
				if (findFirst.isPresent()) {
					embedding.addAll(findFirst.get().getEmbedding());
				}
				break;
			} catch (Exception e) {
				logContainer.funcLogger().severe("Error while invoking OpenAI.", e);
				cosmosDBUtil.updateStatus(uuid,
						CosmosDBDocumentStatus.RETRY_OAI_INVOCATION, logContainer.cosmosLogger());
				retryCount++;
				retrySleep();
			}
		}
		return embedding;
	}

	// 入力文字列を7500文字前後で分割し、句読点で区切られた部分で分割を行います。
	// トークンは 8192 で 8000 で分割した経験上では命令を出す際にオーバフローすることがあるため
	private List<String> splitText(String text, int maxLength) {
		List<String> chunks = new ArrayList<>();
		int textLength = text.length();

		while (textLength > maxLength) {
			int splitIndex = findSplitIndex(text, maxLength);
			chunks.add(text.substring(0, splitIndex));
			text = text.substring(splitIndex);
			textLength = text.length();
		}
		chunks.add(text);
		return chunks;
	}

	// 入力文字列を7500文字の前後で分割し、区切り文字（。？！など）で分割を行います。
	// また、適切な区切り文字が見つからない場合、単純に7500文字ごとに分割されます。
	private int findSplitIndex(String text, int maxLength) {
		// 7200-7500の文字の範囲で区切り文字を探す
		int start = maxLength - 300;
		int splitIndex = maxLength;
		while (splitIndex > start) {
			char c = text.charAt(splitIndex);
			if (isPunctuation(c)) {
				break;
			}
			splitIndex--;
		}
		if (splitIndex == 0) {
			splitIndex = maxLength;
		}
		return splitIndex;
	}

	// 区切り文字の判定
	private boolean isPunctuation(char c) {
		return c == '.' || c == '。' || c == ';' || c == '；' || c == '!' || c == '！' || c == '?'
				|| c == '？';
	}

	private void retrySleep() {
		try {
			TimeUnit.SECONDS.sleep(10);
		} catch (InterruptedException interruptedException) {
			interruptedException.printStackTrace();
			Thread.currentThread().interrupt();
		}
	}

}
