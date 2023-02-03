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
package com.marklogic.client.ext.datamovement.util;

import com.marklogic.client.document.ServerTransform;

public abstract class TransformPropertyValueParser {

	/**
	 * Utility method for parsing a value which may have transform parameters appended - e.g.
	 * myTransform,param1,value1,param2,value2.
	 *
	 * @param value
	 * @return
	 */
	public static ServerTransform parsePropertyValue(String value) {
		String[] tokens = value.split(",");
		ServerTransform transform = new com.marklogic.client.document.ServerTransform(tokens[0]);
		for (int i = 1; i < tokens.length; i += 2) {
			transform.addParameter(tokens[i], tokens[i + 1]);
		}
		return transform;
	}
}
