package com.marklogic.client.ext.datamovement.job;

import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.client.datamovement.*;
import com.marklogic.client.document.DocumentManager;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.document.DocumentWriteOperation.OperationType;
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.client.ext.datamovement.AbstractDataMovementTest;
import com.marklogic.client.ext.datamovement.consumer.WriteRowToHyperConsumer;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.StringQueryDefinition;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.client.query.StructuredQueryDefinition;
import com.tableau.hyperapi.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class ExportRowsToHyperTest extends AbstractDataMovementTest {

  private static String templateNameUri = "customersName.tde";
  private static String templatePrimeUri = "customersPrime.tde";
  private static String masterDetail2Uri = "masterDetail2.tde";
  private static String masterDetail4Uri = "masterDetail4.tde";
  private String datasource = "src/tableau/test/resources/";
  private static String tableauHyperFileName = "generated.hyper";
  private static String schemaName = "Extract";
  private static String tableName = "Customer";
  private static DataMovementManager moveMgr;
  private int batchSize = 3;
  private int threadCount = 3;

  @Before
  public void moreSetup() throws IOException {
    moveMgr = client.newDataMovementManager();
    String[] customers = {
        "<customers> <customer> <name>Alice</name> <id>1</id> <phone>8793993333</phone> <state>CA</state> <isPrime>true</isPrime> <rating>4.3</rating> </customer> "
            + "<customer> <name>Steve</name> <id>9</id> <phone>9999999999</phone> <state>CA</state> <isPrime>true</isPrime> <rating>5</rating> </customer> </customers>",
        "<customers> <name>Bob</name> <id>2</id> <phone>8793993334</phone> <state>AZ</state> <isPrime>true</isPrime> <rating>0</rating> </customers>",
        "<customers> <name>Carl</name> <id>3</id> <phone>8793993335</phone> <state>NY</state> <isPrime>true</isPrime> <rating>4</rating> </customers>",
        "<customers></customers>",
        "<customers> <name>Dennis</name> <id>4</id> <phone>8793993336</phone> <state>WA</state> <isPrime>false</isPrime> <rating>0.0</rating></customers>",
        "<customers> <name>Evelyn</name> <id>5</id> <phone>8793993337</phone> <state>NJ</state> <isPrime>false</isPrime> <rating>3.5</rating></customers>",
        "<customers> <name>John</name> <id>6</id> <phone>8793993338</phone> <state>NJ</state> <isPrime>false</isPrime><rating>1.5</rating> </customers>",
        "<customers> <name>Albert</name> <id>7</id> <phone>8793993339</phone> <state>MA</state> <isPrime>false</isPrime> <rating>3</rating></customers>",
        "<customers> <name>Evelyn</name> <id>8</id> <phone>8793993340</phone> <state>NJ</state> <isPrime>true</isPrime> <rating>2.85</rating></customers>",
        "<customers></customers>" };
    DocumentManager docMgr = client.newDocumentManager();
    DocumentMetadataHandle metadataHandle = new DocumentMetadataHandle();
    metadataHandle.getCollections().add("tableauTest");
    List<DocumentWriteOperation> writeList = new ArrayList<>();
    writeList.add(getDocumentWriteOperation("/tde/customer1.xml", customers[0], "XML"));
    writeList.add(getDocumentWriteOperation("/tde/customer2.xml", customers[1], "XML"));
    writeList.add(getDocumentWriteOperation("/tde/customer3.xml", customers[2], "XML"));
    writeList.add(getDocumentWriteOperation("/tde/customer4.xml", customers[3], "XML"));
    writeList.add(getDocumentWriteOperation("/tde/customer5.xml", customers[4], "XML"));
    writeList.add(getDocumentWriteOperation("/tde/customer6.xml", customers[5], "XML"));
    writeList.add(getDocumentWriteOperation("/tde/customer7.xml", customers[6], "XML"));
    writeList.add(getDocumentWriteOperation("/tde/customer8.xml", customers[7], "XML"));
    writeList.add(getDocumentWriteOperation("/tde/customer9.xml", customers[8], "XML"));
    String jsonDoc1 = "{" +
        "  \"sets2\": {" +
        "    \"masterSet\": {" +
        "      \"master\": [" +
        "        {" +
        "          \"id\": 3," +
        "          \"name\": \"Master 3\"," +
        "          \"date\": \"2016-03-01\"" +
        "        }," +
        "        {" +
        "          \"id\": 4," +
        "          \"name\": \"Master 4\"," +
        "          \"date\": \"2016-05-25\"" +
        "        }" +
        "      ]" +
        "    }," +
        "    \"detailSet\": {" +
        "      \"detail\": [" +
        "        {" +
        "          \"id\": 7," +
        "          \"name\": \"Detail 7\"," +
        "          \"masterId\": \"3\"," +
        "          \"amount\": \"64.33\"," +
        "          \"color\": \"red\"" +
        "        }," +
        "        {" +
        "          \"id\": 8," +
        "          \"name\": \"Detail 8\"," +
        "          \"masterId\": \"4\"," +
        "          \"amount\": \"89.36\"," +
        "          \"color\": \"blue\"" +
        "        }," +
        "        {" +
        "          \"id\": 9," +
        "          \"name\": \"Detail 9\"," +
        "          \"masterId\": \"1\"," +
        "          \"amount\": \"72.90\"," +
        "          \"color\": \"yellow\"" +
        "        }," +
        "        {" +
        "          \"id\": 10," +
        "          \"name\": \"Detail 10\"," +
        "          \"masterId\": \"2\"," +
        "          \"amount\": \"30.26\"," +
        "          \"color\": \"black\"" +
        "        }," +
        "        {" +
        "          \"id\": 11," +
        "          \"name\": \"Detail 11\"," +
        "          \"masterId\": \"3\"," +
        "          \"amount\": \"82.04\"," +
        "          \"color\": \"green\"" +
        "        }," +
        "        {" +
        "          \"id\": 12," +
        "          \"name\": \"Detail 12\"," +
        "          \"masterId\": \"1\"," +
        "          \"amount\": \"25.86\"," +
        "          \"color\": \"red\"" +
        "        }" +
        "      ]" +
        "    }" +
        "  }" +
        "}";
    String jsonDoc2 = "{" +
        "  \"sets4\": {" +
        "    \"masterSet4\": {" +
        "      \"master4\": [" +
        "        {" +
        "          \"id\": 100," +
        "          \"name\": \"Master 100\"," +
        "          \"date\": \"2016-03-11\"" +
        "        }," +
        "        {" +
        "          \"id\": 200," +
        "          \"name\": \"Master 200\"," +
        "          \"date\": \"2016-04-02\"" +
        "        }" +
        "      ]" +
        "    }," +
        "    \"detailSet4\": {" +
        "      \"detail4\": [" +
        "        {" +
        "          \"id\": 100," +
        "          \"name\": \"Detail 100\"," +
        "          \"masterId\": \"100\"," +
        "          \"amount\": \"64.33\"," +
        "          \"color\": \"red\"" +
        "        }," +
        "        {" +
        "          \"id\": 200," +
        "          \"name\": \"Detail 200\"," +
        "          \"masterId\": \"200\"," +
        "          \"amount\": \"89.36\"," +
        "          \"color\": \"blue\"" +
        "        }," +
        "        {" +
        "          \"id\": 300," +
        "          \"name\": \"Detail 300\"," +
        "          \"masterId\": \"200\"," +
        "          \"amount\": \"72.90\"," +
        "          \"color\": \"yellow\"" +
        "        }" +
        "      ]" +
        "    }" +
        "  }" +
        "}";
    String nonMatchDoc = "{\"id\":300, \"RouteName\":\"Dumbarton Express\"}";
    for (int i = 0; i < 200; i++) {
      if (i % 5 == 0) {
        // To make sure we don't have empty rows in the Tableau Extract output file
        writeList.add(getDocumentWriteOperation("/empty/" + i + ".json", "{}", "JSON"));
      }
      if (i % 10 == 0) {
        // To make sure we don't have non Match rows in the Tableau Extract output file
        writeList.add(getDocumentWriteOperation("/nonMatchDoc/" + i + ".json", nonMatchDoc, "JSON"));
      }

      writeList.add(getDocumentWriteOperation("/masterSet2/" + i + ".json", jsonDoc1, "JSON"));
      writeList.add(getDocumentWriteOperation("/masterSet4/" + i + ".json", jsonDoc2, "JSON"));
    }
    writeDocuments(writeList);

    ObjectMapper mapper = new ObjectMapper().configure(Feature.ALLOW_SINGLE_QUOTES, true);
    docMgr.writeAs(templateNameUri, metadataHandle,
        mapper.readTree("{ 'template':{ 'description':'test template', 'context':'//name', "
            + "    'rows':[ { 'schemaName':'customer', 'viewName':'customerName',"
            + "      'columns':[ { 'name':'firstName', 'scalarType':'string', 'val':'.' },"
            + "                  { 'name':'id', 'scalarType':'int', 'val':'../id'},"
            + "                  { 'name':'state', 'scalarType':'string', 'val':'../state'},"
            + "                  { 'name':'rating', 'scalarType':'double', 'val':'../rating'},"
            + "                  { 'name':'phone', 'scalarType':'string', 'val':'../phone'}" + " ] } ] } }"));
    docMgr.writeAs(templatePrimeUri, metadataHandle,
        mapper.readTree("{ 'template':{ 'description':'test template', 'context':'//name', "
            + "    'rows':[ { 'schemaName':'customer', 'viewName':'customerPrime',"
            + "      'columns':[ { 'name':'firstName', 'scalarType':'string', 'val':'.' },"
            + "                  { 'name':'id', 'scalarType':'int', 'val':'../id'},"
            + "                  { 'name':'isPrime', 'scalarType':'boolean', 'val':'../isPrime'}" + " ] } ] } }"));
    docMgr.writeAs(masterDetail2Uri, metadataHandle, mapper.readTree("{ 'template': {" +
        "    'context': '/sets2'," +
        "    'templates': [{" +
        "      'context': 'masterSet/master'," +
        "      'rows': [{" +
        "        'schemaName': 'opticFunctionalTest2'," +
        "        'viewName': 'master'," +
        "        'columns': [{" +
        "          'name': 'id'," +
        "          'scalarType': 'int'," +
        "          'val': 'id'" +
        "        }, {" +
        "          'name': 'name'," +
        "          'scalarType': 'string'," +
        "          'val': 'name'" +
        "        }, {" +
        "          'name': 'date'," +
        "          'scalarType': 'date'," +
        "          'val': 'date'" +
        "        }]" +
        "      }]" +
        "    }, {" +
        "      'context': 'detailSet/detail'," +
        "      'rows': [{" +
        "        'schemaName': 'opticFunctionalTest2'," +
        "        'viewName': 'detail'," +
        "        'columns': [{" +
        "          'name': 'id'," +
        "          'scalarType': 'int'," +
        "          'val': 'id'" +
        "        }, {" +
        "          'name': 'name'," +
        "          'scalarType': 'string'," +
        "          'val': 'name'" +
        "        }, {" +
        "          'name': 'masterId'," +
        "          'scalarType': 'int'," +
        "          'val': 'masterId'" +
        "        }, {" +
        "          'name': 'amount'," +
        "          'scalarType': 'double'," +
        "          'val': 'amount'" +
        "        }, {" +
        "          'name': 'color'," +
        "          'scalarType': 'string'," +
        "          'val': 'color'" +
        "        }]" +
        "      }]" +
        "    }]" +
        "  }" +
        "}"));
    docMgr.writeAs(masterDetail4Uri, metadataHandle, mapper.readTree("{'template': {" +
        "    'context': '/sets4'," +
        "    'templates': [{" +
        "      'context': 'masterSet4/master4'," +
        "      'rows': [{" +
        "        'schemaName': 'opticFunctionalTest4'," +
        "        'viewName': 'master4'," +
        "        'columns': [{" +
        "          'name': 'id'," +
        "          'scalarType': 'int'," +
        "          'val': 'id'" +
        "        }, {" +
        "          'name': 'name'," +
        "          'scalarType': 'string'," +
        "          'val': 'name'" +
        "        }, {" +
        "          'name': 'date'," +
        "          'scalarType': 'date'," +
        "          'val': 'date'" +
        "        }]" +
        "      }]" +
        "    }, {" +
        "      'context': 'detailSet4/detail4'," +
        "      'rows': [{" +
        "        'schemaName': 'opticFunctionalTest4'," +
        "        'viewName': 'detail4'," +
        "        'columns': [{" +
        "          'name': 'id'," +
        "          'scalarType': 'int'," +
        "          'val': 'id'" +
        "        }, {" +
        "          'name': 'name'," +
        "          'scalarType': 'string'," +
        "          'val': 'name'" +
        "        }, {" +
        "          'name': 'masterId'," +
        "          'scalarType': 'int'," +
        "          'val': 'masterId'" +
        "        }, {" +
        "          'name': 'amount'," +
        "          'scalarType': 'double'," +
        "          'val': 'amount'" +
        "        }, {" +
        "          'name': 'color'," +
        "          'scalarType': 'string'," +
        "          'val': 'color'" +
        "        }]" +
        "      }]" +
        "    }]" +
        "  }" +
        "}"));
    docMgr.writeAs("dataTypeCheck.tde", metadataHandle, mapper.readTree(
        "{ 'template': {" +
            "    'context': '/sets2'," +
            "    'templates': [{" +
            "      'context': 'masterSet/master'," +
            "      'rows': [{" +
            "        'schemaName': 'opticFunctionalTest2'," +
            "        'viewName': 'master'," +
            "        'columns': [{" +
            "'name': 'id'," +
            "'scalarType': 'int'," +
            "'val': 'id'" +
            "}, {" +
            "'name': 'route'," +
            "'scalarType': 'string'," +
            "'val': 'route'" +
            "}, {" +
            "'name': 'distance'," +
            "'scalarType': 'double'," +
            "'val': 'distance'" +
            "}, {" +
            "'name': 'clean'," +
            "'scalarType': 'boolean'," +
            "'val': 'clean'" +
            "}]" +
            "}]" +
            "}]}}"
    ));
  }

  private DocumentWriteOperation getDocumentWriteOperation(String uri, String content, String format) {
    Format withFormat = format.equals("XML") ? Format.XML : Format.JSON;
    DocumentMetadataHandle metadataHandle = new DocumentMetadataHandle();
    metadataHandle.getCollections().add("tableauTest");
    return new DocumentWriteOperationImpl(OperationType.DOCUMENT_WRITE, uri, metadataHandle,
        new StringHandle(content).withFormat(withFormat));
  }

  @Test
  public void testGenerateTableauHyperFile() throws Exception {
    logger.info("testGenerateTableauHyperFile() start...");
    File hyperFile = new File(tableauHyperFileName);
    hyperFile.delete();
    logger.info( "init consumer with columns");
    WriteRowToHyperConsumer tableauWriter = null;
    tableauWriter = new WriteRowToHyperConsumer(tableauHyperFileName, schemaName, tableName)
        .withColumn("firstName", SqlType.text())
        .withColumn("id", SqlType.integer())
        .withColumn("state", SqlType.text())
        .withColumn("phone", SqlType.text())
        .withColumn("rating", SqlType.doublePrecision())
        .withColumn("isPrime", SqlType.bool());
    StructuredQueryDefinition query = new StructuredQueryBuilder().directory(1, "/tde/");
    QueryBatcher qb = moveMgr.newQueryBatcher(query)
        .onUrisReady(new ExtractRowsViaTemplateListener()
            .withTemplate(templateNameUri)
            .withTemplate(templatePrimeUri)
            .onTypedRowReady(row -> logger.info("row:" + row))
            .onTypedRowReady(tableauWriter))
        .withBatchSize(batchSize)
        .withThreadCount(threadCount);
    logger.info("starting job");
    moveMgr.startJob(qb);
    qb.awaitCompletion();
    moveMgr.stopJob(qb);
    logger.info("ended job.");

    Path customerPathToDatabase = Paths.get(tableauHyperFileName).toAbsolutePath();

    logger.info( "Path: " + customerPathToDatabase);

    HyperProcess process = new HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU);
    Connection connection = new Connection(
            process.getEndpoint(),
            customerPathToDatabase.toString());

    Catalog catalog = connection.getCatalog();
    SchemaName extractSchema = new SchemaName(schemaName);
    List<TableName> tablesInDatabase = catalog.getTableNames(extractSchema);
    for (TableName table : tablesInDatabase) {
      TableDefinition tableDefinition = catalog.getTableDefinition(table);
      logger.info( "Table " + table + " has qualified name: " + tableDefinition.getTableName());
    }

    TableDefinition tableDef = new TableDefinition(new TableName(extractSchema, tableName));

    long rowCount = connection.<Long>executeScalarQuery(
      "SELECT COUNT(*) FROM " + tableDef.getTableName()
    ).get();
    logger.info("The number of rows in table " + tableDef.getTableName() + " is " + rowCount + "\n");

    Assert.assertEquals( 18, rowCount);
    Assert.assertTrue( "end of test", true);
  }

  @Test
  public void testInvalidTypes() throws IOException {
    // before long we should probably add support for these types, and just pull them from
    // strings in the tde:node-data-extract output
    File outFile = new File("test.hyper");
    outFile.delete();
    WriteRowToHyperConsumer tableauWriter = new WriteRowToHyperConsumer("test.hyper", "Extract", "Test");
    try {
      tableauWriter.withColumn("dateCol", SqlType.date());
      Assert.fail("should have thrown IllegalStateException because Date type is not yet supported");
    } catch (IllegalStateException e) {
      logger.info("Tableau Data Extract threw expected exception since the type is not yet supported - " + e);
    }
    try {
      tableauWriter.withColumn("dateTimeCol", SqlType.timestamp());
      Assert.fail("should have thrown IllegalStateException because timestamp type is not yet supported");
    } catch (IllegalStateException e) {
      logger.info("Tableau Data Extract threw expected exception since the type is not yet supported - " + e);
    }
    try {
      tableauWriter.withColumn("timeCol", SqlType.time());
      Assert.fail("should have thrown IllegalStateException because time type is not yet supported");
    } catch (IllegalStateException e) {
      logger.info("Tableau Data Extract threw expected exception since the type is not yet supported - " + e);
    }
    try {
      tableauWriter.withColumn("geographyCol", SqlType.geography());
      Assert.fail("should have thrown IllegalStateException because geography type is not yet supported");
    } catch (IllegalStateException e) {
      logger.info("Tableau Data Extract threw expected exception since the type is not yet supported - " + e);
    }
    outFile.delete();
    tableauWriter.close();
  }

  @Test
  public void testMismatchedTypes() throws IOException {
    File outFile = new File("test.hyper");
    outFile.delete();
    WriteRowToHyperConsumer tableauWriter = new WriteRowToHyperConsumer("test.hyper", "Extract", "Test")
        .withColumn("integerCol", SqlType.integer());
    PlanBuilder pb = client.newRowManager().newPlanBuilder();
    TypedRow values = new TypedRow("tempURI", "2");
    values.put("integerCol", pb.xs.booleanVal(true));
    try {
      tableauWriter.accept(values);
      Assert.fail("should have thrown IllegalStateException because the types are not compatible");
    } catch (IllegalStateException e) {
      logger.info("Tableau Data Extract threw expected exception since the types are not compatible - " + e);
    }
    outFile.delete();
    tableauWriter.close();
  }

  // QA Functional Tests

  // Test with a single nested templates file.
  @Test
  public void testOneNestedTemplatesFile() throws IOException {
    logger.info("In testOneNestedTemplatesFile method");

    String extractedOutFileName = "TableauOutput.hyper";

    WriteRowToHyperConsumer tableauWriter = null;
    ExtractRowsViaTemplateListener extractor = null;

    StringBuilder batchResults = new StringBuilder();
    StringBuilder batchDetails = new StringBuilder();
    StringBuilder secondConsumerDetails = new StringBuilder();
    try {
      // Delete any existing extracted file
      if ( new File(extractedOutFileName).exists() )
        new File(extractedOutFileName).delete();

      QueryManager queryMgr = client.newQueryManager();
      tableauWriter = new WriteRowToHyperConsumer(extractedOutFileName, "Extract", "Test").withColumn("id", SqlType.integer())
          .withColumn("name", SqlType.text()).withColumn("date", SqlType.text())
          .withColumn("amount", SqlType.doublePrecision()).withColumn("masterId", SqlType.integer())
          .withColumn("color", SqlType.text());

      extractor = new ExtractRowsViaTemplateListener();
      extractor.withTemplate("masterDetail4.tde");

      StructuredQueryBuilder qb = queryMgr.newStructuredQueryBuilder();
      StructuredQueryDefinition querydef = qb.or(qb.directory(true, "/masterSet2/"),
          qb.directory(true, "/masterSet4/"));

      Consumer<TypedRow> consumer = row -> {
        // Iterate over the rows and do the JUnit asserts.
        System.out.print("A new row received");
        System.out.print(" " + row.toString());

        secondConsumerDetails.append(row.toString());
      };

      QueryBatcher queryBatcher = moveMgr.newQueryBatcher(querydef)
          .onUrisReady(extractor.onTypedRowReady(tableauWriter)
              // Use the second consumer to do the asserts in the
              // lambda.
              .onTypedRowReady(consumer))

          .onUrisReady(batch -> {
            for (String str : batch.getItems()) {
              batchResults.append(str).append('|');
              // Batch details
              batchDetails.append(batch.getJobBatchNumber()).append('|').append(batch.getJobResultsSoFar()).append('|')
                  .append(batch.getForestBatchNumber());
            }
          });

      moveMgr.startJob(queryBatcher);
      queryBatcher.awaitCompletion();
    } catch (Exception ex) {
      logger.info("Exceptions thrown " + ex.toString());
    } finally {
      if ( tableauWriter != null ) {
        tableauWriter.close();
        try {
          extractor.close();
        } catch (Exception e) {
          logger.info("Exceptions thrown during ExtractViaTemplateListener close.");
          e.printStackTrace();
        }
      }
    }

    logger.info("batchResults are " + batchResults.toString());
    logger.info("batchDetails are " + batchDetails.toString());

    // Verify that rows are returned from both templates of the TDE file.
    String rowDetails = secondConsumerDetails.toString();
    Assert.assertTrue("Row returned not correct", rowDetails.contains("id=100, name=Master 100"));
    Assert.assertTrue("Row returned not correct", rowDetails.contains("id=200, name=Master 200, date=2016-04-02"));
    Assert.assertTrue("Row returned not correct",
        rowDetails.contains("id=100, name=Detail 100, masterId=100, amount=64.33, color=red"));
    Assert.assertTrue("Row returned not correct",
        rowDetails.contains("id=200, name=Detail 200, masterId=200, amount=89.36, color=blue"));
    Assert.assertTrue("Row returned not correct",
        rowDetails.contains("id=300, name=Detail 300, masterId=200, amount=72.9, color=yellow"));
  }

  // Test with a multiple nested templates file.
  @Test
  public void testMultipleNestedTemplatesFiles() throws IOException {
    logger.info("In testMultipleNestedTemplatesFiles method");

    String extractedOutFileName = "TableauOutput.hyper";

    WriteRowToHyperConsumer tableauWriter = null;
    ExtractRowsViaTemplateListener extractor = null;

    StringBuilder batchResults = new StringBuilder();
    StringBuilder secondConsumerDetails = new StringBuilder();
    try {
      // Delete any existing extracted file
      if ( new File(extractedOutFileName).exists() )
        new File(extractedOutFileName).delete();

      QueryManager queryMgr = client.newQueryManager();
      tableauWriter = new WriteRowToHyperConsumer(extractedOutFileName, "Extract", "Test").withColumn("id", SqlType.integer())
          .withColumn("name", SqlType.text()).withColumn("date", SqlType.text())
          .withColumn("amount", SqlType.doublePrecision()).withColumn("masterId", SqlType.integer())
          .withColumn("color", SqlType.text());

      extractor = new ExtractRowsViaTemplateListener();
      extractor.withTemplate("masterDetail4.tde").withTemplate("masterDetail2.tde");

      StructuredQueryBuilder qb = queryMgr.newStructuredQueryBuilder();
      StructuredQueryDefinition querydef = qb.or(qb.directory(true, "/masterSet2/"),
          qb.directory(true, "/masterSet4/"));

      Consumer<TypedRow> consumer = row -> {
        // Iterate over the rows and do the JUnit asserts.
        System.out.print("A new row received");
        System.out.print(" " + row.toString());

        secondConsumerDetails.append(row.toString());
      };

      QueryBatcher queryBatcher = moveMgr.newQueryBatcher(querydef)
          .onUrisReady(extractor.onTypedRowReady(tableauWriter)
              // Use the second consumer to do the asserts in the
              // lambda.
              .onTypedRowReady(consumer))

          .onUrisReady(batch -> {
            for (String str : batch.getItems()) {
              batchResults.append(str).append('|');
            }
          });

      moveMgr.startJob(queryBatcher);
      queryBatcher.awaitCompletion();
    } catch (Exception ex) {
      logger.info("Exceptions thrown " + ex.toString());
    } finally {
      if ( tableauWriter != null ) {
        tableauWriter.close();
        try {
          extractor.close();
        } catch (Exception e) {
          logger.info("Exceptions thrown during ExtractRowsViaTemplateListener close.");
          e.printStackTrace();
        }
      }
    }

    // Verify the results
    logger.info("batchResults are " + batchResults.toString());

    // Verify that rows are returned from both templates of the TDE file.
    String rowDetails = secondConsumerDetails.toString();

    logger.info("rowDetails are " + rowDetails.toString());

    Assert.assertTrue("Row returned not correct", rowDetails.contains("id=100, name=Master 100"));
    Assert.assertTrue("Row returned not correct", rowDetails.contains("id=200, name=Master 200, date=2016-04-02"));
    Assert.assertTrue("Row returned not correct",
        rowDetails.contains("id=100, name=Detail 100, masterId=100, amount=64.33, color=red"));
    Assert.assertTrue("Row returned not correct",
        rowDetails.contains("id=200, name=Detail 200, masterId=200, amount=89.36, color=blue"));
    Assert.assertTrue("Row returned not correct",
        rowDetails.contains("id=300, name=Detail 300, masterId=200, amount=72.9, color=yellow"));
  }

  @Test
  public void testIncorrectColumnType() throws IOException {
    logger.info("In testIncorrectColumnType method");

    String extractedOutFileName = "TableauOutput.hyper";
    WriteRowToHyperConsumer tableauWriter = null;
    StringBuilder acceptResults = new StringBuilder();

    // Delete any existing extracted file
    if ( new File(extractedOutFileName).exists() )
      new File(extractedOutFileName).delete();

    PlanBuilder pb = client.newRowManager().newPlanBuilder();

    QueryManager queryMgr = client.newQueryManager();
    tableauWriter = new WriteRowToHyperConsumer(extractedOutFileName, "Extracted", "Test").withColumn("id", SqlType.doublePrecision())
        .withColumn("name", SqlType.text()).withColumn("date", SqlType.text())
        .withColumn("amount", SqlType.doublePrecision()).withColumn("masterId", SqlType.integer())
        .withColumn("color", SqlType.text());

    ExtractRowsViaTemplateListener extractor = new ExtractRowsViaTemplateListener();
    extractor.withTemplate("masterDetail4.tde");

    TypedRow incorrectType = new TypedRow("name", "Master 100");
    incorrectType.put("id", pb.xs.string("random"));

    try {
      tableauWriter.accept(incorrectType);
    } catch (Exception ex) {
      logger.info("Exceptions thrown " + ex.toString());
      acceptResults.append(ex.toString());
    } finally {
      if ( tableauWriter != null ) {
        tableauWriter.close();
        try {
          extractor.close();
        } catch (Exception e) {
          logger.info("Exceptions thrown during ExtractRowsViaTemplateListener close.");
          e.printStackTrace();
        }
      }
    }
    String res = acceptResults.toString();
    Assert.assertTrue("URI returned not correct", res.contains("java.lang.IllegalStateException"));
    Assert.assertTrue("URI returned not correct", res.contains("Column \"id\" type DOUBLE is incompatible with data type"));
  }

  // Test with different number of columns for WriteRowToTableauConsumer
  // instance
  @Test
  public void testNumberOfColumns() throws IOException {
    logger.info("In testNumberOfColumns method");

    String extractedOutFileName = "TableauOutput.hyper";

    WriteRowToHyperConsumer tableauWriter = null;
    ExtractRowsViaTemplateListener extractor = null;

    StringBuilder batchResults = new StringBuilder();
    StringBuilder batchDetails = new StringBuilder();
    StringBuilder secondConsumerDetails = new StringBuilder();

    QueryManager queryMgr = client.newQueryManager();
    try {
      // Delete any existing extracted file
      if ( new File(extractedOutFileName).exists() )
        new File(extractedOutFileName).delete();

      // No columns specified.
      tableauWriter = new WriteRowToHyperConsumer(extractedOutFileName, "Extract", "Test");

      extractor = new ExtractRowsViaTemplateListener();
      extractor.withTemplate("masterDetail4.tde");

      StructuredQueryBuilder qb = queryMgr.newStructuredQueryBuilder();
      StructuredQueryDefinition querydef = qb.or(qb.directory(true, "/masterSet2/"),
          qb.directory(true, "/masterSet4/"));

      Consumer<TypedRow> consumer = row -> {
        // Iterate over the rows and do the JUnit asserts.
        System.out.print("A new row received ");
        System.out.print(" " + row.toString());

        secondConsumerDetails.append(row.toString());
      };

      QueryBatcher queryBatcher = moveMgr.newQueryBatcher(querydef)
          .onUrisReady(extractor.onTypedRowReady(tableauWriter)
              // Use the second consumer to do the asserts in the
              // lambda.
              .onTypedRowReady(consumer))

          .onUrisReady(batch -> {
            for (String str : batch.getItems()) {
              batchResults.append(str).append('|');
            }
          });

      moveMgr.startJob(queryBatcher);
      queryBatcher.awaitCompletion();
    } catch (Exception ex) {
      logger.info("Exceptions thrown " + ex.toString());
    } finally {
      if ( tableauWriter != null ) {
        tableauWriter.close();
        try {
          extractor.close();
        } catch (Exception e) {
          logger.info("Exceptions thrown during ExtractRowsViaTemplateListener close.");
          e.printStackTrace();
        }
      }
    }

    // Verify the results. The results from second consumer will remain the
    // same. Tableau o/p will be different.
    // Columns from first rows will be in the Tableau output.
    logger.info("batchResults are " + batchResults.toString());
    logger.info("batchDetails are " + batchDetails.toString());

    // Verify that rows are returned from both templates of the TDE file.
    String rowDetails = secondConsumerDetails.toString();
    Assert.assertTrue("Row returned not correct", rowDetails.contains("id=100, name=Master 100"));
    Assert.assertTrue("Row returned not correct", rowDetails.contains("id=200, name=Master 200, date=2016-04-02"));
    Assert.assertTrue("Row returned not correct",
        rowDetails.contains("id=100, name=Detail 100, masterId=100, amount=64.33, color=red"));
    Assert.assertTrue("Row returned not correct",
        rowDetails.contains("id=200, name=Detail 200, masterId=200, amount=89.36, color=blue"));
    Assert.assertTrue("Row returned not correct",
        rowDetails.contains("id=300, name=Detail 300, masterId=200, amount=72.9, color=yellow"));
  }

  // Test with different number of columns for WriteRowToTableauConsumer
  // instance
  @Test
  public void testInCorrectNoTemplate() throws IOException {
    logger.info("In testInCorrectNoTemplate method");

    String extractedOutFileName = "TableauOutput.hyper";

    WriteRowToHyperConsumer tableauWriter = null;
    ExtractRowsViaTemplateListener extractor = null;

    StringBuilder batchResults = new StringBuilder();

    QueryManager queryMgr = client.newQueryManager();
    try {
      // Delete any existing extracted file
      if ( new File(extractedOutFileName).exists() )
        new File(extractedOutFileName).delete();

      tableauWriter = new WriteRowToHyperConsumer(extractedOutFileName, "Extract", "Test")
          .withColumn("id", SqlType.integer())
          .withColumn("name", SqlType.text()).withColumn("date", SqlType.text())
          .withColumn("amount", SqlType.doublePrecision()).withColumn("masterId", SqlType.integer())
          .withColumn("color", SqlType.text());

      extractor = new ExtractRowsViaTemplateListener();
      extractor.withTemplate("AAA.tde");

      StructuredQueryBuilder qb = queryMgr.newStructuredQueryBuilder();
      StructuredQueryDefinition querydef = qb.or(qb.directory(true, "/masterSet2/"),
          qb.directory(true, "/masterSet4/"));

      QueryBatcher queryBatcher = moveMgr.newQueryBatcher(querydef)
          .onUrisReady(extractor.onFailure((batch, throwable) -> {
            logger.info("Exceptions thrown from Extractor " + throwable.toString());
            batchResults.append(throwable.toString());
          }));

      moveMgr.startJob(queryBatcher);
      queryBatcher.awaitCompletion();
    } catch (Exception ex) {
      logger.info("Exceptions thrown " + ex.toString());
    } finally {
      if ( tableauWriter != null ) {
        tableauWriter.close();
        try {
          extractor.close();
        } catch (Exception e) {
          logger.info("Exceptions thrown during ExtractRowsViaTemplateListener close.");
          e.printStackTrace();
        }
      }
    }
    String strResults = batchResults.toString();
    Assert.assertTrue("Exception not correct", strResults.contains("com.marklogic.client.ResourceNotFoundException"));
    Assert.assertTrue("Exception not correct", strResults.contains("Resource or document does not exist"));
    Assert.assertTrue("Exception not correct", strResults.contains("Cannot find template: AAA.tde"));
  }

  // Test with a multiple nested templates and multiple extractors to a Tableau
  // Extractor file.
  @Test
  public void testMultipleExtractors() throws IOException {
    logger.info("In testMultipleExtractors method");

    String extractedOutFileName = "TableauOutput.hyper";
    String extractedOutFileName1 = "TableauOutput1.hyper";
    String extractedOutFileName2 = "TableauOutput2.hyper";

    WriteRowToHyperConsumer tableauWriter1 = null;
    WriteRowToHyperConsumer tableauWriter2 = null;
    ExtractRowsViaTemplateListener extractor1 = null;
    ExtractRowsViaTemplateListener extractor2 = null;

    StringBuilder batchResults = new StringBuilder();
    StringBuilder failureDetails = new StringBuilder();
    StringBuilder secondConsumerDetails = new StringBuilder();
    try {
      // Delete any existing extracted file
      if ( new File(extractedOutFileName).exists() )
        new File(extractedOutFileName).delete();
      if ( new File(extractedOutFileName1).exists() )
        new File(extractedOutFileName1).delete();
      if ( new File(extractedOutFileName2).exists() )
        new File(extractedOutFileName2).delete();

      QueryManager queryMgr = client.newQueryManager();

      tableauWriter1 = new WriteRowToHyperConsumer(extractedOutFileName1, "Extract", "Test")
          .withColumn("id", SqlType.integer())
          .withColumn("name", SqlType.text());
      tableauWriter2 = new WriteRowToHyperConsumer(extractedOutFileName2, "Extract", "Test")
          .withColumn("date", SqlType.text())
          .withColumn("amount", SqlType.doublePrecision())
          .withColumn("masterId", SqlType.integer())
          .withColumn("color", SqlType.text());

      extractor1 = new ExtractRowsViaTemplateListener();
      extractor1.withTemplate("masterDetail4.tde").withTemplate("masterDetail2.tde");

      extractor2 = new ExtractRowsViaTemplateListener();
      extractor2.withTemplate("masterDetail4.tde").withTemplate("masterDetail2.tde");

      StructuredQueryBuilder qb = queryMgr.newStructuredQueryBuilder();
      StructuredQueryDefinition querydef = qb.or(qb.directory(true, "/masterSet2/"),
          qb.directory(true, "/masterSet4/"));

      Consumer<TypedRow> consumer = row -> {
        // Iterate over the rows and do the JUnit asserts.
        System.out.print("A new row received");
        System.out.print(" " + row.toString());

        secondConsumerDetails.append(row.toString());
      };

      // Verify with two consumers on one file
      String sameFileExcep = null;
      ExtractRowsViaTemplateListener extractor3 = new ExtractRowsViaTemplateListener();
      extractor3.withTemplate("masterDetail4.tde").withTemplate("masterDetail2.tde");
      ExtractRowsViaTemplateListener extractor4 = new ExtractRowsViaTemplateListener();
      extractor4.withTemplate("masterDetail4.tde").withTemplate("masterDetail2.tde");
      WriteRowToHyperConsumer tableauWriter3 = null;
      WriteRowToHyperConsumer tableauWriter4 = null;

      try {
        tableauWriter3 = new WriteRowToHyperConsumer(extractedOutFileName, "Extract", "Test")
            .withColumn("id", SqlType.integer())
            .withColumn("name", SqlType.text());
        tableauWriter4 = new WriteRowToHyperConsumer(extractedOutFileName, "Extract", "Test")
            .withColumn("date", SqlType.text())
            .withColumn("amount", SqlType.doublePrecision())
            .withColumn("masterId", SqlType.integer())
            .withColumn("color", SqlType.text());
        QueryBatcher queryBatcherErr = moveMgr.newQueryBatcher(querydef)
            .onUrisReady(extractor3.onTypedRowReady(tableauWriter3).onFailure((batch, throwable) -> {

              logger.info("Exceptions thrown from Extractor with  queryBatcherErr" + throwable.toString());
              failureDetails.append(throwable.toString());
            })).onUrisReady(extractor4.onTypedRowReady(tableauWriter4));
        moveMgr.startJob(queryBatcherErr);
        queryBatcherErr.awaitCompletion();

      } catch (Exception e) {
        sameFileExcep = e.getMessage();
        logger.info(sameFileExcep.toString());
        String str = "Filename \"" + extractedOutFileName + "\" already exists";
        Assert.assertTrue("Exception not correct", sameFileExcep.contains(str));
      } finally {
        if ( tableauWriter3 != null ) {
          tableauWriter3.close();
          try {
            extractor3.close();
          } catch (Exception e) {
            logger.info("Exceptions thrown during ExtractRowsViaTemplateListener 3 close.");
            e.printStackTrace();
          }
        }
        if ( tableauWriter4 != null ) {
          tableauWriter4.close();
          try {
            extractor4.close();
          } catch (Exception e) {
            logger.info("Exceptions thrown during ExtractRowsViaTemplateListener 4 close.");
            e.printStackTrace();
          }
        }
      }
      // Multiple extractors working into a TDE file

      // Writer1
      QueryBatcher queryBatcher = moveMgr.newQueryBatcher(querydef)
          .onUrisReady(extractor1.onTypedRowReady(tableauWriter1).onFailure((batch, throwable) -> {
            // In ideal case there should not be any errors / exceptions.
            logger.info("Exceptions thrown from Extractor " + throwable.toString());
            failureDetails.append(throwable.toString());
          })

              // Use the second consumer to do the asserts in the
              // lambda.
              .onTypedRowReady(consumer))

          .onUrisReady(batch -> {
            for (String str : batch.getItems()) {
              batchResults.append(str).append('|');
            }
          });
      // Writer2
      queryBatcher.onUrisReady(extractor2.onTypedRowReady(tableauWriter2));

      moveMgr.startJob(queryBatcher);
      queryBatcher.awaitCompletion();
    } catch (Exception ex) {
      logger.info("Exceptions thrown " + ex.toString());
    } finally {
      if ( tableauWriter1 != null ) {
        tableauWriter1.close();
        try {
          extractor1.close();
        } catch (Exception e) {
          logger.info("Exceptions thrown during ExtractRowsViaTemplateListener close.");
          e.printStackTrace();
        }
      }
      if ( tableauWriter2 != null ) {
        tableauWriter2.close();
        try {
          extractor2.close();
        } catch (Exception e) {
          logger.info("Exceptions thrown during ExtractRowsViaTemplateListener close.");
          e.printStackTrace();
        }
      }
    }

    logger.info("Failure Listenerd details (if any)  are " + failureDetails.toString());
    Assert.assertTrue("Tableau extract failed due to errors/exception ", failureDetails.toString().isEmpty());

    // Validate the bytes of the output files. Visual inspection using Tableau
    // Desktop software done on both files.
    byte[] extractedBytes1 = Files.readAllBytes(new File(extractedOutFileName1).toPath());
    byte[] extractedBytes2 = Files.readAllBytes(new File(extractedOutFileName2).toPath());

    Assert.assertTrue("Tableau extracted " + extractedOutFileName1 + " file available", extractedBytes1.length > 0);
    Assert.assertTrue("Tableau extracted " + extractedOutFileName2 + " file available", extractedBytes2.length > 0);
    logger.info("batchResults are " + batchResults.toString());

    // Verify that rows are returned from both templates of the TDE file.
    String rowDetails = secondConsumerDetails.toString();
    Assert.assertTrue("Row returned not correct", rowDetails.contains("id=100, name=Master 100"));
    Assert.assertTrue("Row returned not correct", rowDetails.contains("id=200, name=Master 200, date=2016-04-02"));
    Assert.assertTrue("Row returned not correct",
        rowDetails.contains("id=100, name=Detail 100, masterId=100, amount=64.33, color=red"));
    Assert.assertTrue("Row returned not correct",
        rowDetails.contains("id=200, name=Detail 200, masterId=200, amount=89.36, color=blue"));
    Assert.assertTrue("Row returned not correct",
        rowDetails.contains("id=300, name=Detail 300, masterId=200, amount=72.9, color=yellow"));
  }

  @Test
  public void testMultipleThreadsOnQB() throws KeyManagementException, NoSuchAlgorithmException, IOException, SAXException, ParserConfigurationException
  {
    logger.info("In testOneNestedTemplatesFile method");

    String extractedOutFileName = "TableauOutput.hyper";
    StringHandle handle = new StringHandle();
    DocumentMetadataHandle metadataHandle = new DocumentMetadataHandle();
    metadataHandle.getCollections().add("tableauTest");
    WriteBatcher batcher = moveMgr.newWriteBatcher();

    String dataTypes = "{\"sets2\": {" +
        "\"masterSet\": {" +
        "\"master\": {" +
        "\"id\":300," +
        "\"route\":\"Dumbarton Express\"," +
        "\"clean\":true," +
        "\"distance\":20.95" +
        "}}}}";

    handle.set(dataTypes);
    for (int i = 0; i < 200; i++) {
      batcher.add("/dataTypes" + i + ".json", metadataHandle, handle);
    }
    // Flush
    batcher.flushAndWait();

    WriteRowToHyperConsumer tableauWriter = null;
    ExtractRowsViaTemplateListener extractor = null;

    StringBuilder batchResults = new StringBuilder();
    StringBuilder batchDetails = new StringBuilder();
    StringBuilder secondConsumerDetails = new StringBuilder();
    try {
      // Delete any existing extracted file
      if ( new File(extractedOutFileName).exists() )
        new File(extractedOutFileName).delete();

      QueryManager queryMgr = client.newQueryManager();
      tableauWriter = new WriteRowToHyperConsumer(extractedOutFileName, "Extract", "Test")
          .withColumn("id", SqlType.integer())
          .withColumn("route", SqlType.text())
          .withColumn("distance", SqlType.doublePrecision())
          .withColumn("clean", SqlType.bool());

      extractor = new ExtractRowsViaTemplateListener();
      extractor.withTemplate("dataTypeCheck.tde");

      StringQueryDefinition querydef = queryMgr.newStringDefinition();
      querydef.setCriteria("Dumbarton");

      Consumer<TypedRow> consumer = row -> {
        // Iterate over the rows and do the JUnit asserts.
        System.out.print("A new row received");
        System.out.print(" " + row.toString());

        secondConsumerDetails.append(row.toString());
      };

      QueryBatcher queryBatcher = moveMgr.newQueryBatcher(querydef).withThreadCount(3)
          .onUrisReady(extractor.onTypedRowReady(tableauWriter)
              // Use the second consumer to do the asserts in the
              // lambda.
              .onTypedRowReady(consumer))

          .onUrisReady(batch -> {
            for (String str : batch.getItems()) {
              batchResults.append(str).append('|');
              // Batch details
              batchDetails.append(batch.getJobBatchNumber()).append('|').append(batch.getJobResultsSoFar()).append('|')
                  .append(batch.getForestBatchNumber());
            }
          });

      moveMgr.startJob(queryBatcher);
      queryBatcher.awaitCompletion();
    } catch (Exception ex) {
      logger.info("Exceptions thrown " + ex.toString());
    } finally {
      if ( tableauWriter != null ) {
        tableauWriter.close();
        try {
          extractor.close();
        } catch (Exception e) {
          logger.info("Exceptions thrown during ExtractRowsViaTemplateListener close.");
          e.printStackTrace();
        }
      }
    }

    // Verify the results
    logger.info("batchResults are " + batchResults.toString());
    logger.info("batchDetails are " + batchDetails.toString());

    // Verify that rows are returned from both templates of the TDE file.
    String rowDetails = secondConsumerDetails.toString();
    Assert.assertTrue("Row returned not correct", rowDetails.contains("id=300, route=Dumbarton Express, distance=20.95, clean=true"));
  }

  @After
  public void tearDown() {
    QueryBatcher qb = moveMgr.newQueryBatcher(new StructuredQueryBuilder().collection("tableauTest"))
        .onUrisReady(new DeleteListener())
        .withConsistentSnapshot();
    moveMgr.startJob(qb);
    qb.awaitCompletion();
    moveMgr.stopJob(qb);
  }
}
