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

import com.marklogic.client.ext.datamovement.listener.SetPermissionsListener;

import java.util.Arrays;

public class SetPermissionsJob extends AbstractQueryBatcherJob {

	private String[] rolesAndCapabilities;

	public SetPermissionsJob() {
		super();

		addRequiredJobProperty("permissions",
			"Comma-delimited list of roles and capabilities defining permissions to set on selected records",
			value -> setRolesAndCapabilities(value.split(",")));
	}

	public SetPermissionsJob(String... rolesAndCapabilities) {
		this();
		setRolesAndCapabilities(rolesAndCapabilities);
	}

	@Override
	protected String getJobDescription() {
		return "Setting permissions " + Arrays.asList(rolesAndCapabilities) + " on documents " + getQueryDescription();
	}

	public void setRolesAndCapabilities(String... rolesAndCapabilities) {
		this.rolesAndCapabilities = rolesAndCapabilities;
		this.addUrisReadyListener(new SetPermissionsListener(rolesAndCapabilities));
	}
}
