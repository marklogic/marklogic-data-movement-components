/*
 * Copyright (c) 2023 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.client.ext.datamovement.consumer;

import com.marklogic.client.document.DocumentRecord;
import com.marklogic.client.io.InputStreamHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;

/**
 * Consumer implementation that is intended to be used with DMSDK's ExportListener. Writes each document to a File based
 * on the directory passed to this class's constructor plus the document's URI.
 */
public class WriteDocumentToFileConsumer implements Consumer<DocumentRecord> {

	protected Logger logger = LoggerFactory.getLogger(getClass());

	private File baseDir;
	private boolean logErrors = true;

	public WriteDocumentToFileConsumer(File baseDir) {
		this.baseDir = baseDir;
		this.baseDir.mkdirs();
	}

	@Override
	public void accept(DocumentRecord documentRecord) {
		String uri = documentRecord.getUri();
		File outputFile = getOutputFile(documentRecord);
		if (logger.isDebugEnabled()) {
			logger.debug("Writing document with URI " + uri + " to file: " + outputFile);
		}
		try {
			writeDocumentToFile(documentRecord, outputFile);
		} catch (IOException e) {
			String message = "Unable to write document to file; URI: " + uri + "; file: " + outputFile;
			if (logErrors) {
				logger.warn(message, e);
			} else {
				throw new RuntimeException(message, e);
			}
		}
	}

	protected File getOutputFile(DocumentRecord documentRecord) {
		return new File(baseDir, documentRecord.getUri());
	}

	protected void writeDocumentToFile(DocumentRecord documentRecord, File file) throws IOException {
		file.getParentFile().mkdirs();
		FileOutputStream fos = new FileOutputStream(file);
		try {
			InputStream in = documentRecord.getContent(new InputStreamHandle()).get();
			byte[] buffer = new byte[4096];
			int bytesRead = -1;
			while ((bytesRead = in.read(buffer)) != -1) {
				fos.write(buffer, 0, bytesRead);
			}
			fos.flush();
		} finally {
			fos.close();
		}
	}

	protected File getBaseDir() {
		return baseDir;
	}

	protected boolean isLogErrors() {
		return logErrors;
	}

	public void setLogErrors(boolean logErrors) {
		this.logErrors = logErrors;
	}
}
