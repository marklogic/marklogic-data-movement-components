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
package com.marklogic.client.ext.datamovement.listener;

import com.marklogic.client.datamovement.ExportListener;
import com.marklogic.client.datamovement.QueryBatch;
import com.marklogic.client.ext.datamovement.consumer.WriteToZipConsumer;

import java.io.File;

/**
 * Listener for exporting each QueryBatch to a separate zip file, with each file being written to a given File
 * directory. Reuses ExportListener and WriteToZipConsumer for writing to each zip file.
 */
public class ExportBatchesToZipsListener extends AbstractExportBatchesListener {

	private File exportDir;

	private boolean flattenUri = false;
	private String uriPrefix;

	public ExportBatchesToZipsListener(File exportDir) {
		withFilenameExtension(".zip");
		this.exportDir = exportDir;
		this.exportDir.mkdirs();
	}

	/**
	 * Exports the batch to a single zip file.
	 *
	 * @param queryBatch
	 */
	@Override
	protected void exportBatch(QueryBatch queryBatch) {
		File file = getFileForBatch(queryBatch, exportDir);
		WriteToZipConsumer consumer = new WriteToZipConsumer(file);
		prepareWriteToZipConsumer(consumer);

		ExportListener listener = new ExportListener();
		listener.onDocumentReady(consumer);
		prepareExportListener(listener);

		listener.processEvent(queryBatch);
		consumer.close();
	}

	/**
	 * Prepares the consumer based on the properties set on this class. Can be overridden by a subclass to perform
	 * additional preparation.
	 *
	 * @param writeToZipConsumer
	 */
	protected void prepareWriteToZipConsumer(WriteToZipConsumer writeToZipConsumer) {
		writeToZipConsumer.setFlattenUri(this.flattenUri);
		if (uriPrefix != null) {
			writeToZipConsumer.setUriPrefix(uriPrefix);
		}
	}

	public ExportBatchesToZipsListener withFlattenUri(boolean flattenUri) {
		this.flattenUri = flattenUri;
		return this;
	}

	public ExportBatchesToZipsListener withUriPrefix(String uriPrefix) {
		this.uriPrefix = uriPrefix;
		return this;
	}
}
