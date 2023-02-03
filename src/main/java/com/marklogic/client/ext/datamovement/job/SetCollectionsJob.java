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

import com.marklogic.client.ext.datamovement.listener.SetCollectionsListener;

import java.util.Arrays;

public class SetCollectionsJob extends AbstractQueryBatcherJob {

	private String[] collections;

	public SetCollectionsJob() {
		super();

		addRequiredJobProperty("collections", "Comma-delimited list collections to set on selected records",
			value -> setCollections(value.split(",")));
	}

	public SetCollectionsJob(String... collections) {
		this();
		setCollections(collections);
	}

	@Override
	protected String getJobDescription() {
		return "Setting collections " + Arrays.asList(collections) + " on documents " + getQueryDescription();
	}

	public void setCollections(String... collections) {
		this.collections = collections;
		this.addUrisReadyListener(new SetCollectionsListener(collections));
	}
}
