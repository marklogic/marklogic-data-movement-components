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

import com.marklogic.client.eval.EvalResult;

import java.util.Iterator;

/**
 * Adapts an Iterator of EvalResults to an Iterator of Strings so that it can be used easily with a DMSDK QueryBatcher.
 */
public class EvalResultIterator implements Iterator<String> {

	private Iterator<EvalResult> evalResults;

	public EvalResultIterator(Iterator<EvalResult> evalResults) {
		this.evalResults = evalResults;
	}

	@Override
	public boolean hasNext() {
		return evalResults.hasNext();
	}

	@Override
	public String next() {
		return evalResults.next().getString();
	}
}
