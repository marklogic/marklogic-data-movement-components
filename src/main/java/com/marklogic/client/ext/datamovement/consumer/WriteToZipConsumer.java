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
import com.marklogic.client.io.BytesHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.function.Consumer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Intended to be used with DMSDK's ExportListener class. Supports writing all documents to a single zip file.
 *
 * If a File is used to construct this class, be sure to call close() after all of the documents have been written, so
 * that the ZipOutputStream that is opened on the File is properly closed.
 *
 * If a ZipOutputStream is used to construct this class, it is up to the client to determine when to close it.
 */
public class WriteToZipConsumer implements Consumer<DocumentRecord>, Closeable {

	protected Logger logger = LoggerFactory.getLogger(getClass());

	private ZipOutputStream zipOutputStream;
	private boolean flattenUri = false;
	private String uriPrefix;

	public WriteToZipConsumer(File file) {
		try {
			this.zipOutputStream = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
		} catch (IOException e) {
			throw new RuntimeException("Unable to open zip output stream on file: " + file + "; cause: " + e.getMessage(), e);
		}
	}

	public WriteToZipConsumer(ZipOutputStream zipOutputStream) {
		this.zipOutputStream = zipOutputStream;
	}

	@Override
	public void accept(DocumentRecord documentRecord) {
		String uri = documentRecord.getUri();
		ZipEntry zipEntry = buildZipEntry(documentRecord);
		synchronized (this.zipOutputStream) {
			try {
				zipOutputStream.putNextEntry(zipEntry);
				if (logger.isDebugEnabled()) {
					logger.debug("Writing zip entry, name: " + zipEntry.getName());
				}
				zipOutputStream.write(documentRecord.getContent(new BytesHandle()).get());
				zipOutputStream.closeEntry();
			} catch (IOException e) {
				throw new RuntimeException("Unable to write zip entry for URI: " + uri + "; cause: " + e.getMessage(), e);
			}
		}
	}

	@Override
	public void close() {
		if (zipOutputStream != null) {
			try {
				zipOutputStream.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	protected ZipEntry buildZipEntry(DocumentRecord documentRecord) {
		String uri = documentRecord.getUri();
		if (flattenUri) {
			int pos = uri.lastIndexOf("/");
			uri = pos > -1 ? uri.substring(pos + 1) : uri;
		}

		if (uriPrefix != null) {
			uri = uriPrefix + uri;
		}

		return new ZipEntry(uri);
	}

	public void setFlattenUri(boolean flattenUri) {
		this.flattenUri = flattenUri;
	}

	public void setUriPrefix(String uriPrefix) {
		this.uriPrefix = uriPrefix;
	}
}
