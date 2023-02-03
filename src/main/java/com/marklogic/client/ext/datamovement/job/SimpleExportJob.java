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
package com.marklogic.client.ext.datamovement.job;

import com.marklogic.client.datamovement.ExportListener;
import com.marklogic.client.document.DocumentRecord;

import java.util.function.Consumer;

/**
 * Simple job that uses ExportListener with zero or more Consumer objects.
 */
public class SimpleExportJob extends AbstractQueryBatcherJob {

	private ExportListener exportListener;

	public SimpleExportJob(Consumer<DocumentRecord>... consumers) {
		exportListener = new ExportListener();
		for (Consumer<DocumentRecord> consumer : consumers) {
			exportListener.onDocumentReady(consumer);
		}
		this.addUrisReadyListener(exportListener);
	}

	@Override
	protected String getJobDescription() {
		return "Exporting documents " + getQueryDescription();
	}

	/**
	 * Allow for a client to fiddle with the ExportListener created by this class.
	 *
	 * @return
	 */
	public ExportListener getExportListener() {
		return exportListener;
	}
}
