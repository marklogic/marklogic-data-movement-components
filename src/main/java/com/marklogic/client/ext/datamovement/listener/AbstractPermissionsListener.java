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

import com.marklogic.client.datamovement.QueryBatch;
import com.marklogic.client.datamovement.QueryBatchListener;
import com.marklogic.client.eval.ServerEvaluationCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractPermissionsListener implements QueryBatchListener {

	protected Logger logger = LoggerFactory.getLogger(getClass());

	private String[] rolesAndCapabilities;

	public AbstractPermissionsListener(String... rolesAndCapabilities) {
		this.rolesAndCapabilities = rolesAndCapabilities;
	}

	protected abstract String getXqueryFunction();

	@Override
	public void processEvent(QueryBatch queryBatch) {
		String[] uris = queryBatch.getItems();
		StringBuilder sb = new StringBuilder();

		ServerEvaluationCall call = queryBatch.getClient().newServerEval();

		for (int i = 0; i < uris.length; i++) {
			sb.append("declare variable $uri" + i + " external;\n");
			call.addVariable("uri" + i, uris[i]);
		}

		StringBuilder permissionSequence = new StringBuilder("(");
		for (int j = 0; j < rolesAndCapabilities.length; j += 2) {
			sb.append("declare variable $role" + j + " external;\n");
			sb.append("declare variable $capability" + j + " external;\n");
			call.addVariable("role" + j, rolesAndCapabilities[j]);
			call.addVariable("capability" + j, rolesAndCapabilities[j + 1]);
			if (j > 0) {
				permissionSequence.append(", ");
			}
			permissionSequence.append("xdmp:permission($role").append(j).append(", $capability").append(j).append(")");
		}
		permissionSequence.append(")");

		final String function = getXqueryFunction();

		for (int i = 0; i < uris.length; i++) {
			if (i > 0) {
				sb.append(",\n");
			}
			sb.append(String.format("%s($uri%d, %s)", function, i, permissionSequence));
		}

		if (logger.isDebugEnabled()) {
			logger.debug("Executing: " + sb);
		}

		call.xquery(sb.toString());
		call.eval();
	}
}

