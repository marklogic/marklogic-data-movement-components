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

import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.JobTicket;
import com.marklogic.client.datamovement.QueryBatcher;

/**
 * Receipt-style object for QueryBatcherTemplate methods. Intended to give the client control over how the job is stopped,
 * if it hasn't been already, as well as the JobTicket so that other job information can be retrieved.
 */
public class QueryBatcherJobTicket {

	private DataMovementManager dataMovementManager;
	private QueryBatcher queryBatcher;
	private JobTicket jobTicket;

	public QueryBatcherJobTicket(DataMovementManager dataMovementManager, QueryBatcher queryBatcher, JobTicket jobTicket) {
		this.dataMovementManager = dataMovementManager;
		this.queryBatcher = queryBatcher;
		this.jobTicket = jobTicket;
	}

	public DataMovementManager getDataMovementManager() {
		return dataMovementManager;
	}

	public QueryBatcher getQueryBatcher() {
		return queryBatcher;
	}

	public JobTicket getJobTicket() {
		return jobTicket;
	}
}
