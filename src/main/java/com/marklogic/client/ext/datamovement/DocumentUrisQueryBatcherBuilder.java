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
package com.marklogic.client.ext.datamovement;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.QueryBatcher;
import com.marklogic.client.query.StructuredQueryDefinition;

/**
 * Builds a QueryBatcher based on an array of document URIs.
 */
public class DocumentUrisQueryBatcherBuilder implements QueryBatcherBuilder {

	private String[] documentUris;

	public DocumentUrisQueryBatcherBuilder(String... documentUris) {
		this.documentUris = documentUris;
	}

	@Override
	public QueryBatcher buildQueryBatcher(DatabaseClient databaseClient, DataMovementManager dataMovementManager) {
		StructuredQueryDefinition query = databaseClient.newQueryManager().newStructuredQueryBuilder().document(documentUris);
		return dataMovementManager.newQueryBatcher(query);
	}
}
