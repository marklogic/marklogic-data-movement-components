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
import com.marklogic.client.eval.EvalResult;
import com.marklogic.client.ext.datamovement.util.EvalResultIterator;

import java.util.Iterator;

/**
 * Builds a QueryBatcher based on a URI pattern that is fed into cts:uri-match via an eval call. Note that cts:uri-match
 * may not always scale as well as a cts:uris query will.
 */
public class UriPatternQueryBatcherBuilder implements QueryBatcherBuilder {

	private String uriPattern;

	public UriPatternQueryBatcherBuilder(String uriPattern) {
		this.uriPattern = uriPattern;
	}

	@Override
	public QueryBatcher buildQueryBatcher(DatabaseClient databaseClient, DataMovementManager dataMovementManager) {
		final Iterator<EvalResult> evalResults = databaseClient.newServerEval().xquery(String.format("cts:uri-match('%s')", uriPattern)).eval().iterator();
		return dataMovementManager.newQueryBatcher(new EvalResultIterator(evalResults));
	}
}
