package com.marklogic.client.ext.datamovement.consumer;

import com.marklogic.client.MarkLogicIOException;
import com.marklogic.client.datamovement.TypedRow;
import com.marklogic.client.type.*;
import com.tableau.hyperapi.*;
//import com.tableausoftware.common.Collation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.function.Consumer;

/**
 * This is a Consumer which takes in a TypedRow and writes the row data
 * extracted via ExtractViaTemplateListener to a file of format Tableau Hyper file. It can be initialized as follows:
 *
 * <pre>{@code
 * WriteRowToTableauConsumer tableauWriter = new WriteRowToTableauConsumer("output.tde")
 *    .withColumn("firstName", Type.UNICODE_STRING)
 *    .withColumn("salary", Type.INTEGER);
 * }</pre>
 *
 *
 * This consumer depends on certain Tableau SDK for creating the Tableau Hyper file. For more details on how to install the SDK and using the
 * Tableau jars, please visit the following link:
 *
 * <a href="https://onlinehelp.tableau.com/current/api/sdk/en-us/help.htm">Tableau SDK</a>
 *
 * This will work with MarkLogic server version greater than or equal to 9.0.5.
 *
 */
public class WriteRowToHyperConsumer
    implements Consumer<TypedRow>, AutoCloseable
{
  protected Logger logger = LoggerFactory.getLogger(getClass());

  private boolean initialized = false;
  private SchemaName SCHEMA_NAME;
  private TableDefinition tableDef;
  private LinkedHashMap<String, WriteRowToHyperConsumer.Column> columns = new LinkedHashMap<>();
  HyperProcess process;
  private Connection connection;

  /**
   * Initialize the Consumer with the Tableau TDE file to which the rows should
   * be written.
   *
   * @param tableauHyperFileName the tableauHyperFileName specifies the tableau
   *          hyper file we'll write to
   */
  public WriteRowToHyperConsumer(String tableauHyperFileName, String schemaName, String tableName) {
    logger.info("WriteRowToHyperConsumer(): " + tableauHyperFileName);
    if ( new File(tableauHyperFileName).exists() ) {
      throw new IllegalStateException("Filename \"" + tableauHyperFileName + "\" already exists");
    }
    try {
      // Starts the Hyper Process with telemetry enabled to send data to Tableau.
      // To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
      process = new HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU);

      Path customerDatabasePath = Paths.get(tableauHyperFileName);
      logger.info( "Path: " + customerDatabasePath);

      connection = new Connection(
        process.getEndpoint(),
        customerDatabasePath.toString(),
        CreateMode.CREATE_AND_REPLACE
      );

      SCHEMA_NAME = new SchemaName(schemaName);

      Catalog catalog = connection.getCatalog();
      catalog.createSchema(SCHEMA_NAME);
      tableDef = new TableDefinition(new TableName(SCHEMA_NAME, tableName));
    }
    catch (Exception e) {
      throw new MarkLogicIOException(e);
    }
  }

  /**
   * The accept function is compatible with ExtractViaTemplateListener. It
   * consumes the TypedRow object emitted by ExtractViaTemplateListener and
   * writes it to the Tableau TDE file.
   *
   * @see Consumer#accept(Object)
   */
  public void accept(TypedRow vals) {
    synchronized (this) {
      TableDefinition tdef = getTableDef(vals);
      Inserter inserter = new Inserter(connection, tdef);
      for (WriteRowToHyperConsumer.Column column : columns.values()) {
        XsAnyAtomicTypeVal value = vals.get(column.columnName);
        if (value != null) {
          if (SqlType.bool().equals( column.type)) {
            if (value instanceof XsBooleanVal) {
              inserter.add(((XsBooleanVal) value).getBoolean());
              continue;
            }
          } else if (SqlType.text().equals(column.type)) {
            if (value instanceof XsStringVal) {
              inserter.add(((XsStringVal) value).getString());
              continue;
            }
          } else if (SqlType.doublePrecision().equals( column.type)) {
            if (value instanceof XsShortVal) {
              inserter.add(((XsShortVal) value).getBigInteger().shortValueExact());
              continue;
            } else if (value instanceof XsIntVal) {
              inserter.add(((XsIntegerVal) value).getBigInteger().intValueExact());
              continue;
            } else if (value instanceof XsIntegerVal) {
              inserter.add(((XsIntegerVal) value).getBigInteger().intValueExact());
              continue;
            } else if (value instanceof XsLongVal) {
              inserter.add(((XsIntegerVal) value).getBigInteger().longValueExact());
              continue;
            } else if (value instanceof XsFloatVal) {
              inserter.add(((XsFloatVal) value).getFloat());
              continue;
            } else if (value instanceof XsDoubleVal) {
              inserter.add(((XsDoubleVal) value).getDouble());
              continue;
            }
          } else if (SqlType.integer().equals( column.type)) {
            if (value instanceof XsShortVal) {
              inserter.add(((XsShortVal) value).getBigInteger().shortValueExact());
              continue;
            } else if (value instanceof XsIntVal) {
              inserter.add(((XsIntegerVal) value).getBigInteger().intValueExact());
              continue;
            } else if (value instanceof XsIntegerVal) {
              inserter.add(((XsIntegerVal) value).getBigInteger().intValueExact());
              continue;
            } else if (value instanceof XsLongVal) {
              inserter.add(((XsIntegerVal) value).getBigInteger().longValueExact());
              continue;
            }
          }
          // we already validated the column types when we added them, so the value
          // types must not be matching
          throw new IllegalStateException("Column \"" + column.columnName + "\" type " +
                  column.type + " is incompatible with data type \"" + value.getClass().getName() + "\"");
        }
        else {
          inserter.addNull();
          continue;
        }
      }
      inserter.endRow();
      inserter.execute();
    }
  }

  /**
   * Get the Table definition. If it is not initialized, initialize and return.
   *
   * @param vals the TypedRow object used to initialize the Table Definition.
   * @return the TableDefinition
   */
  private TableDefinition getTableDef(TypedRow vals) {
    initialize(vals);
    return tableDef;
  }

  /**
   * Initialize the table definition with columns from the TypedRow object
   *
   * @param vals the TypedRow object used to initialize the Table Definition.
   */
  private void initialize(TypedRow vals) {
    if ( initialized == false ) {
      logger.info("initialize()");
      synchronized(this) {
        if ( initialized == false ) {
          try {
            if ( columns.size() == 0 ) {
              for (String columnName : vals.keySet()) {
                logger.info("Adding column: " + columnName);
                XsAnyAtomicTypeVal value = vals.get(columnName);
                if (value instanceof XsBooleanVal) {
                  withColumn(columnName, SqlType.bool());
                }
                else if (value instanceof XsStringVal) {
                  withColumn(columnName, SqlType.text());
                }
                else if (value instanceof XsIntegerVal ||
                  value instanceof XsIntVal ||
                  value instanceof XsShortVal ||
                  value instanceof XsLongVal ) {
                  withColumn(columnName, SqlType.integer());
                }
                else if (value instanceof XsFloatVal ||
                  value instanceof XsDoubleVal ) {
                  withColumn(columnName, SqlType.doublePrecision());
                }
                else {
                  throw new IllegalStateException("Unsupported type: " + value.getClass().getName());
                }
              }
            }
            for ( WriteRowToHyperConsumer.Column column : columns.values() ) {
                tableDef.addColumn(column.columnName, column.type);
            }
            connection.getCatalog().createTable( tableDef);
            initialized = true;
          } catch (Exception e) {
            throw new MarkLogicIOException(e);
          }
        }
      }
    }
  }

  /**
   * Add a column to the Consumer to add it in the Tableau Data Extract file
   *
   * @param columnName the column name specified in the Template Driven
   *          Extraction template (the column names are case sensitive)
   * @param type the Tableau Data Type
   * @return the Consumer for chaining
   */
  public WriteRowToHyperConsumer withColumn(String columnName, SqlType type) {
    checkSupportedTypes(type);
    columns.put(columnName, new WriteRowToHyperConsumer.Column(columnName, type));
    return this;
  }

  /**
   * Closes underlying TableDefinition and Extract. This should be called by the
   * QueryBatchListener (ExtractViaTemplateListener in this case).
   *
   * @see java.lang.AutoCloseable#close()
   */
  @Override
  public void close() {
    process.close();
    connection.close();
  }

  private void checkSupportedTypes(SqlType type) {
    if ( !type.equals(SqlType.bool()) &&
         !type.equals(SqlType.doublePrecision()) &&
         !type.equals(SqlType.integer()) &&
         !type.equals(SqlType.text()) ) {
      throw new IllegalStateException("Type " + type + " is not yet supported");
    }
  }

  private class Column {
    String columnName;
    SqlType type;

    Column(String columnName, SqlType type) {
      this.columnName = columnName;
      this.type = type;
    }
  }
}
