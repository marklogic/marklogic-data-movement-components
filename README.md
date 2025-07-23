# Archived repository

This repository has been archived. All classes have been migrated to the 
[ml-javaclient-util subproject in the ml-gradle repository](https://github.com/marklogic/ml-gradle/tree/master/ml-javaclient-util). 
The classes will be available in the upcoming ml-gradle 6.0.0 release.  

# Reusable components for the MarkLogic Data Movement SDK

The [Data Movement SDK](http://docs.marklogic.com/guide/java/data-movement) (DMSDK) is intended for manipulating large numbers
of documents through an asynchronous interface that efficiently distributes workload across a MarkLogic cluster. This 
project then provides reusable components that depend on DMSDK and make it easier to quickly create applications with DMSDK.

For information on integration with [Tableau](https://www.tableau.com/), please see the [README file](https://github.com/marklogic-community/marklogic-data-movement-components/blob/master/src/tableau/README.md). 

The current focus of this project is simplifying the usage of a QueryBatcher via a class that implements the QueryBatcherJob interface:

    QueryBatcherJobTicket run(DatabaseClient client);

Job usage should generally look like this:

    DatabaseClient client = ... // create this any way you'd like
    new AddCollectionsJob("blue", "green")
      .setWhereCollections("red")
      .run(client);

The intent is that an implementation of QueryBatcherJob can be instantiated with its required arguments, and then (if needed) a "setWhere*" method can be used to specify the URIs to select. Then, call "run(client)" to run the job. The Job classes will typically reuse the Listener implementations that were originally added in version 3.0.0, with new ones being added in subsequent releases. 

Each QueryBatcherJob implementation is likely to extend AbstractQueryBatcherJob, which provides access to a number of methods for configuring the job:

    new AddCollectionsJob("blue", "green")
      .setAwaitCompletion(true)
      .setConsistentSnapshot(false)
      .setStopJobAfterCompletion(true)
      .setJobName("my-job")
      .setBatchSize(500) // defaults to 100
      .setThreadCount(32) // defaults to 8
      .setForestConfig(new ForestConfiguration(...))
      .run(client);

Several "setWhere*" methods are available as well to select URIs, though only one should be used at a time:

    new AddCollectionsJob("blue", "green")
      .setWhereUris("doc1", "doc2")
      .setWhereCollections("coll1", "coll2")
      .setWhereUriPattern("/test/*.xml")
      .setWhereUrisQuery("cts:element-value-query(xs:QName('hello'), 'world')");

The following jobs exist:

1. AddCollectionsJob
1. AddPermissionsJob
1. DeleteCollectionsJob
1. DeleteJob - added in 1.1
1. ExportBatchesToDirectoryJob
1. ExportBatchesToZipsJob
1. ExportToFileJob
1. ExportToZipJob
1. RemoveCollectionsJob
1. RemovePermissionsJob
1. SetCollectionsJob
1. SetPermissionsJob
1. SimpleExportJob (allows for using any Consumer with DMSDK's ExportListener)
1. SimpleQueryBatcherJob (allows for using any QueryBatchListener)

And of course you can create your own class, which is likely to extend AbstractQueryBatcherJob. 

## Configuring a job via Properties

To simplify using a job in a context like [Gradle](https://github.com/marklogic-community/ml-gradle), a job can implement the ConfigurableJob interface, which means the job can be configured via a Properties object. More importantly, a job can also describe the properties that it supports. The ConfigurableJob interface has the following methods:

    List<String> configureJob(Properties props);
    List<JobProperty> getJobProperties();

Given a Properties instance, a job can be configured via "configureJob", which returns a list of validation error messages - e.g. for missing properties. A tool like ml-gradle can then call "getJobProperties" and print out this list so that a Gradle user knows what properties are available for each job. 

Here's an example of adding collections and configuring the job via a Properties object:

    Properties props = new Properties();
    props.setProperty("collections", "red,green");
    props.setProperty("whereCollections", "test");
    props.setProperty("batchSize", "50");
    AddCollectionsJob job = new AddCollectionsJob();
    job.configureJob(props);
    job.run(databaseClient);

Of course, this is more verbose than simply calling methods directly on the job as shown in the examples at the top of this page. But this allows for a tool like ml-gradle to simply pass all the properties it has to the "configureJob" method and remain unaware of each job's configuration details. 
