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

import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.ext.datamovement.listener.ExportBatchesToDirectoryListener;

import java.io.File;

public class ExportBatchesToDirectoryJob extends AbstractQueryBatcherJob {

	private ExportBatchesToDirectoryListener exportBatchesToDirectoryListener;
	private File exportDir;

	public ExportBatchesToDirectoryJob() {
		super();

		// Need to process this property first so that the listener isn't null
		addRequiredJobProperty("exportPath", "Directory path to which each batch should be written as a file",
			value -> setExportDir(new File(value)));

		addJobProperty("fileHeader", "Content written to the start of each file",
			value -> getExportListener().withFileHeader(value));

		addJobProperty("fileFooter", "Content written to the end of each file",
			value -> getExportListener().withFileFooter(value));

		addJobProperty("filenamePrefix", "Prefix written to the beginning of the filename of each file; defaults to batch-",
			value -> getExportListener().withFilenamePrefix(value));

		addJobProperty("filenameExtension", "Filename extension for each file; defaults to .zip",
			value -> getExportListener().withFilenameExtension(value));

		addJobProperty("recordPrefix", "Optional content to be written before each record is written",
			value -> getExportListener().withRecordPrefix(value));

		addJobProperty("recordSuffix", "Optional content to be written after each record is written",
			value -> getExportListener().withRecordSuffix(value));

		addTransformJobProperty((value, transform) -> getExportListener().withTransform(transform));
	}

	public ExportBatchesToDirectoryJob(File exportDir) {
		this();
		setExportDir(exportDir);
	}

	@Override
	protected String getJobDescription() {
		return "Exporting batches of documents " + getQueryDescription() + " to files at: " + exportDir;
	}

	public ExportBatchesToDirectoryListener getExportListener() {
		return exportBatchesToDirectoryListener;
	}

	public void setExportDir(File exportDir) {
		this.exportDir = exportDir;
		exportBatchesToDirectoryListener = new ExportBatchesToDirectoryListener(exportDir);
		addUrisReadyListener(exportBatchesToDirectoryListener);
	}

	public File getExportDir() {
		return exportDir;
	}
}
