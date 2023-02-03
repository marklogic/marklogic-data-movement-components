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

import com.marklogic.client.ext.datamovement.listener.AddCollectionsListener;

import java.util.Arrays;

public class AddCollectionsJob extends AbstractQueryBatcherJob implements QueryBatcherJob {

	private String[] collections;

	public AddCollectionsJob() {
		super();
		addRequiredJobProperty("collections", "Comma-delimited list collections to which selected records are added",
			value -> setCollections(value.split(",")));
	}

	public AddCollectionsJob(String... collections) {
		this();
		setCollections(collections);
	}

	@Override
	protected String getJobDescription() {
		return "Adding documents " + getQueryDescription() + " to collections " + Arrays.asList(collections);
	}

	public void setCollections(String... collections) {
		this.collections = collections;
		this.addUrisReadyListener(new AddCollectionsListener(collections));
	}
}
