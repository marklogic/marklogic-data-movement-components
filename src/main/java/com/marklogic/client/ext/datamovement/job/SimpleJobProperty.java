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

import java.util.function.Consumer;

public class SimpleJobProperty implements JobProperty {

	private String name;
	private String description;
	private Consumer<String> propertyValueConsumer;
	private boolean required;

	public SimpleJobProperty(String name, String description, Consumer<String> propertyValueConsumer) {
		this.name = name;
		this.description = description;
		this.propertyValueConsumer = propertyValueConsumer;
	}

	@Override
	public String getPropertyName() {
		return name;
	}

	@Override
	public String getPropertyDescription() {
		return description;
	}

	@Override
	public Consumer<String> getPropertyValueConsumer() {
		return propertyValueConsumer;
	}

	@Override
	public boolean isRequired() {
		return required;
	}

	public void setRequired(boolean required) {
		this.required = required;
	}
}
