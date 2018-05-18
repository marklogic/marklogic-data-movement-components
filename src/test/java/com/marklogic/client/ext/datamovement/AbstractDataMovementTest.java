package com.marklogic.client.ext.datamovement;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.DeleteListener;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.ext.ConfiguredDatabaseClientFactory;
import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.client.ext.DefaultConfiguredDatabaseClientFactory;
import com.marklogic.client.ext.batch.DataMovementBatchWriter;
import com.marklogic.client.ext.batch.SimpleDocumentWriteOperation;
import com.marklogic.client.ext.datamovement.job.DeleteCollectionsJob;
import com.marklogic.client.ext.spring.SpringDatabaseClientConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Abstract class for making it easier to test DMSDK listeners and consumers.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {TestConfig.class})
public abstract class AbstractDataMovementTest extends Assert {

	protected final static String COLLECTION = "data-movement-test";

	protected final static String FIRST_URI = "/test/dmsdk-test-1.xml";
	protected final static String SECOND_URI = "/test/dmsdk-test-2.xml";

	protected QueryBatcherTemplate queryBatcherTemplate;

	protected Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	protected DatabaseClientConfig clientConfig;
	protected DatabaseClient client;

	protected ConfiguredDatabaseClientFactory configuredDatabaseClientFactory = new DefaultConfiguredDatabaseClientFactory();

	protected DatabaseClient newClient() {
		client = configuredDatabaseClientFactory.newDatabaseClient(clientConfig);
		return client;
	}

	@After
	public void releaseClientOnTearDown() {
		if (client != null) {
			try {
				client.release();
			} catch (Exception ex) {
				// That's fine, the test probably released it already
			}
		}
	}

	@Before
	public void setup() {
		queryBatcherTemplate = new QueryBatcherTemplate(newClient("Documents"));
		queryBatcherTemplate.setJobName("manage-collections-test");
		queryBatcherTemplate.setBatchSize(1);
		queryBatcherTemplate.setThreadCount(2);

		queryBatcherTemplate.applyOnDocumentUris(new DeleteListener(), FIRST_URI, SECOND_URI);
		new DeleteCollectionsJob(COLLECTION).run(client);

		writeDocuments(FIRST_URI, SECOND_URI);
	}

	protected DatabaseClient newClient(String database) {
		String currentDatabase = clientConfig.getDatabase();
		clientConfig.setDatabase(database);
		client = configuredDatabaseClientFactory.newDatabaseClient(clientConfig);
		clientConfig.setDatabase(currentDatabase);
		return client;
	}

	protected void writeDocuments(String... uris) {
		List<DocumentWriteOperation> list = new ArrayList<>();
		for (String uri : uris) {
			list.add(new SimpleDocumentWriteOperation(uri, "<test>" + uri + "</test>", COLLECTION));
		}
		writeDocuments(list);
	}

	protected void writeDocuments(List<DocumentWriteOperation> writeOperations) {
		DataMovementBatchWriter writer = new DataMovementBatchWriter(client);
		writer.initialize();
		writer.write(writeOperations);
		writer.waitForCompletion();
	}

	protected void assertZipFileContainsEntryNames(File file, String... names) {
		Set<String> entryNames = new HashSet<>();
		try {
			ZipFile zipFile = new ZipFile(file);
			try {
				Enumeration<?> entries = zipFile.entries();
				while (entries.hasMoreElements()) {
					ZipEntry zipEntry = (ZipEntry) entries.nextElement();
					entryNames.add(zipEntry.getName());
				}
			} finally {
				zipFile.close();
			}
		} catch (IOException ie) {
			throw new RuntimeException(ie);
		}

		logger.info("Entry names: " + entryNames);
		for (String name : names) {
			assertTrue(entryNames.contains(name));
		}
		assertEquals(names.length, entryNames.size());
	}

}

@Configuration
@Import(value = {SpringDatabaseClientConfig.class})
@PropertySource("classpath:application.properties")
class TestConfig {

	/**
	 * Ensures that placeholders are replaced with property values
	 */
	@Bean
	public static PropertySourcesPlaceholderConfigurer propertyPlaceHolderConfigurer() {
		return new PropertySourcesPlaceholderConfigurer();
	}
}

