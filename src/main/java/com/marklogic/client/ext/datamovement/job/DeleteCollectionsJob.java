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
import com.marklogic.client.ext.datamovement.CollectionsQueryBatcherBuilder;
import com.marklogic.client.ext.datamovement.QueryBatcherBuilder;

import java.util.Arrays;

public class DeleteCollectionsJob extends AbstractQueryBatcherJob {

    private String[] collections;

    public DeleteCollectionsJob() {
        super();
        setRequireWhereProperty(false);
        addCollectionsProperty(true);
    }

    /**
     * When this constructor is used - i.e. the collections are known at the time the job is constructed - then
     * "collections" does not become a required property, as it's already been set.
     *
     * @param collections
     */
    public DeleteCollectionsJob(String... collections) {
        super();
        setRequireWhereProperty(false);
        setCollections(collections);
        if (collections != null && collections.length > 0) {
            addCollectionsProperty(false);
        } else {
            addCollectionsProperty(true);
        }
    }

    private void addCollectionsProperty(boolean required) {
        final String message = "Comma-delimited list of collections to delete";
        if (required) {
            addRequiredJobProperty("collections", message, value -> setCollections(value.split(",")));
        } else {
            addJobProperty("collections", message, value -> setCollections(value.split(",")));
        }
    }

    @Override
    protected QueryBatcherBuilder newQueryBatcherBuilder() {
        return new CollectionsQueryBatcherBuilder(collections);
    }

    @Override
    protected String getJobDescription() {
        return "Deleting collections: " + Arrays.asList(collections);
    }

    public void setCollections(String... collections) {
        this.collections = collections;
        this.addUrisReadyListener(new DeleteListener());
    }

    @Override
    protected void addWhereJobProperties() {
        // These don't apply to this job
    }
}
