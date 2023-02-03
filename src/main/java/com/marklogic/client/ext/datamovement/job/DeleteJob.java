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

import com.marklogic.client.datamovement.DeleteListener;

/**
 * Simple job for deleting documents. Uses the DMSDK DeleteListener and requires a "where" property to be set to
 * specify which documents should be deleted.
 */
public class DeleteJob extends AbstractQueryBatcherJob {

	public DeleteJob() {
		setRequireWhereProperty(true);
		this.addUrisReadyListener(new DeleteListener());
	}

	@Override
	protected String getJobDescription() {
		return "Deletes documents matching the query defined by a 'where' property";
	}
}
