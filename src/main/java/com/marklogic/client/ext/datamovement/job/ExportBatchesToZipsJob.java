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
import com.marklogic.client.ext.datamovement.listener.ExportBatchesToZipsListener;

import java.io.File;

public class ExportBatchesToZipsJob extends AbstractQueryBatcherJob {

	private File exportDir;
	private ExportBatchesToZipsListener exportBatchesToZipsListener;

	public ExportBatchesToZipsJob() {
		super();

		addRequiredJobProperty("exportPath", "Directory path to which each batch should be written as a zip",
			value -> setExportDir(new File(value)));

		addJobProperty("filenamePrefix", "Prefix written to the beginning of the filename of each file; defaults to batch-",
			value -> getExportListener().withFilenamePrefix(value));

		addJobProperty("filenameExtension", "Filename extension for each file; defaults to .zip",
			value -> getExportListener().withFilenameExtension(value));
		
		addJobProperty("flattenUri", "Whether or not record URIs are flattened before being used as zip entry names; defaults to false",
			value -> getExportListener().withFlattenUri(Boolean.parseBoolean(value)));

		addTransformJobProperty((value, transform) -> getExportListener().withTransform(transform));

		addJobProperty("uriPrefix", "Prefix to prepend to each URI it is used as an entry name; applied after a URI is optionally flattened",
			value -> getExportListener().withUriPrefix(value));

	}

	public ExportBatchesToZipsJob(File exportDir) {
		this();
		setExportDir(exportDir);
	}

	@Override
	protected String getJobDescription() {
		return "Exporting batches of documents " + getQueryDescription() + " to files at: " + exportDir;
	}

	public ExportBatchesToZipsListener getExportListener() {
		return exportBatchesToZipsListener;
	}

	public void setExportDir(File exportDir) {
		this.exportDir = exportDir;
		this.exportBatchesToZipsListener = new ExportBatchesToZipsListener(exportDir);
		this.addUrisReadyListener(exportBatchesToZipsListener);
	}

	public File getExportDir() {
		return exportDir;
	}
}
