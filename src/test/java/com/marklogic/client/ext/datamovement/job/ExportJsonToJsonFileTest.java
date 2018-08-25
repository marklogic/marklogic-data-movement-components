package com.marklogic.client.ext.datamovement.job;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.ext.batch.SimpleDocumentWriteOperation;
import com.marklogic.client.ext.datamovement.AbstractDataMovementTest;
import org.junit.Before;
import org.junit.Test;
import org.springframework.util.FileCopyUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ExportJsonToJsonFileTest extends AbstractDataMovementTest {

	private ExportToFileJob job;
	private File exportFile;

	@Before
	public void setup() {
		File exportDir = new File("build/export-test/" + System.currentTimeMillis());
		exportFile = new File(exportDir, "exportToFileTest.xml");
		job = new ExportToFileJob(exportFile);
		job.setWhereUris("test1.json", "test2.json");
	}

	@Test
	public void omitLastRecordSuffix() {
		job.setFileHeader("[");
		job.setFileFooter("]");
		job.setRecordSuffix(",");
		job.setOmitLastRecordSuffix(true);

		String exportedJson = runJobAndGetJson();
		assertTrue(exportedJson.contains("{\"uri\":\"test1.json\"},{\"uri\":\"test2.json\"}]"));
	}

	@Test
	public void dontOmitLastRecordSuffix() {
		job.setFileHeader("[");
		job.setFileFooter("]");
		job.setRecordSuffix(",");
		job.setOmitLastRecordSuffix(false);

		String exportedJson = runJobAndGetJson();
		assertTrue(exportedJson.contains("{\"uri\":\"test1.json\"},{\"uri\":\"test2.json\"},]"));
	}

	@Test
	public void useDefaultConstructorAndOmitLastRecordSuffix() {
		job = new ExportToFileJob();
		job.setWhereUris("test1.json", "test2.json");
		job.setFileHeader("[");
		job.setFileFooter("]");
		job.setRecordSuffix(",");
		job.setOmitLastRecordSuffix(true);
		job.setExportFile(exportFile);

		String exportedJson = runJobAndGetJson();
		assertTrue(exportedJson.contains("{\"uri\":\"test1.json\"},{\"uri\":\"test2.json\"}]"));
	}

	@Test
	public void omitLastRecordSuffixWithNoFileFooter() {
		job.setRecordSuffix(",");
		job.setOmitLastRecordSuffix(true);

		String exportedJson = runJobAndGetJson();
		assertTrue(
			"Since no file footer was set, whitespace should be used to overwrite the last record suffix",
			exportedJson.contains("{\"uri\":\"test1.json\"},{\"uri\":\"test2.json\"} ")
		);
	}

	@Override
	protected void writeDocuments(String... uris) {
		List<DocumentWriteOperation> list = new ArrayList<>();
		uris = new String[]{"test1.json", "test2.json"};
		for (String uri : uris) {
			list.add(new SimpleDocumentWriteOperation(uri, "{\"uri\":\"" + uri + "\"}", COLLECTION));
		}
		writeDocuments(list);
	}

	protected String runJobAndGetJson() {
		job.run(client);
		String exportedJson = null;
		try {
			exportedJson = new String(FileCopyUtils.copyToByteArray(exportFile));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		logger.info("Exported JSON: " + exportedJson);
		return exportedJson;
	}
}
