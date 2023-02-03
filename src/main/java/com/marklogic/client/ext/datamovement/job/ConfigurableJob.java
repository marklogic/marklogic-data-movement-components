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

import java.util.List;
import java.util.Properties;

/**
 * Interface for a job to implement when it can be configured via a Properties object. A primary benefit for clients is
 * that they can call the getJobProperties method and e.g. print out this list so a user knows what properties are
 * available for a job.
 */
public interface ConfigurableJob {

	/**
	 * Configure this job with the given set of Properties.
	 *
	 * @return a list of strings, with each presumably being a validation error message
	 */
	List<String> configureJob(Properties props);

	/**
	 * @return the list of JobProperty objects for this job. One use case for this is for a client to print out the
	 * name and description of each property.
	 */
	List<JobProperty> getJobProperties();
}
