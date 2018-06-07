package com.marklogic.client.ext.datamovement.consumer;

import com.marklogic.client.MarkLogicIOException;
import com.marklogic.client.datamovement.TypedRow;
import com.marklogic.client.type.*;
import com.tableausoftware.TableauException;
import com.tableausoftware.common.Collation;
import com.tableausoftware.extract.Extract;
import com.tableausoftware.extract.Row;
import com.tableausoftware.extract.TableDefinition;
import com.tableausoftware.extract.Table;
import com.tableausoftware.common.Type;

import java.io.File;
import java.util.Iterator;
import java.util.LinkedHashMap;


import java.util.function.Consumer;

/**
 * This is a Consumer which takes in a TypedRow and writes the row data
 * extracted via ExtractViaTemplateListener to a file of format Tableau Data
 * Extract. It can be initialized as follows:
 *
 * <pre>{@code
 * WriteRowToTableauConsumer tableauWriter = new WriteRowToTableauConsumer("output.tde")
 *    .withColumn("firstName", Type.UNICODE_STRING)
 *    .withColumn("salary", Type.INTEGER);
 * }</pre>
 *
 * where output.tde file is the output Tableau Data Extract format file
 * which contains the extracted rows. We should have the necessary columns
 * defined with withColumn() method. The column names defined in the Template
 * Driven Extraction templates should match the ones specified in the
 * withColumn() method. Then it can be passed directly to the
 * onTypedRowReady method of ExtractViaTemplateListener method as follows:
 *
 * <pre>{@code
 * ExtractViaTemplateListener listner = new ExtractViaTemplateListener()
 *      .withTemplate(templateUri)
 *      .onTypedRowReady(tableauWriter)
 * }</pre>
 *
 * This consumer depends on certain Tableau SDK for creating the Tableau Data
 * Extract file. For more details on how to install the SDK and using the
 * Tableau jars, please visit the following link:
 *
 * <a href="https://onlinehelp.tableau.com/current/api/sdk/en-us/help.htm">Tableau SDK</a>
 *
 * This will work with MarkLogic server version greater than or equal to 9.0.5.
 *
 */
public class WriteRowToTableauConsumer
    implements Consumer<TypedRow>, AutoCloseable
{
  private boolean initialized = false;
  private TableDefinition tableDef;
  private LinkedHashMap<String,Column> columns = new LinkedHashMap<>();
  private Extract extract;
  private Table table;

  /**
   * Initialize the Consumer with the Tableau TDE file to which the rows should
   * be written.
   *
   * @param tableauExtractFileName the tableauTdeFileName specifies the tableau
   *          data extract file we'll write to
   */
  public WriteRowToTableauConsumer(String tableauExtractFileName) {
    if ( new File(tableauExtractFileName).exists() ) {
      throw new IllegalStateException("Filename \"" + tableauExtractFileName + "\" already exists");
    }
    try {
      extract = new Extract(tableauExtractFileName);
      tableDef = new TableDefinition();
    } catch (TableauException e) {
      throw new MarkLogicIOException(e);
    }
  }

  /**
   * The accept function is compatible with ExtractViaTemplateListener. It
   * consumes the TypedRow object emitted by ExtractViaTemplateListener and
   * writes it to the Tableau TDE file.
   *
   * @see java.util.function.Consumer#accept(java.lang.Object)
   */
  public void accept(TypedRow vals) {
    try {
      Row row = new Row(getTableDef(vals));
      try {
        Iterator<Column> values = columns.values().iterator();
        for ( int index=0; values.hasNext(); index++ ) {
          Column column = values.next();
          XsAnyAtomicTypeVal value = vals.get(column.columnName);
          if ( value != null ) {
            if ( column.type == Type.BOOLEAN ) {
              if ( value instanceof XsBooleanVal ) {
                row.setBoolean(index, ((XsBooleanVal)value).getBoolean());
                continue;
              }
            } else if ( column.type == Type.UNICODE_STRING )
            {
              if ( value instanceof XsStringVal ) {
                row.setString(index, ((XsStringVal)value).getString());
                continue;
              }
            } else if ( column.type == Type.DOUBLE ) {
              if ( value instanceof XsShortVal ) {
                row.setDouble(index, ((XsShortVal)value).getBigInteger().shortValueExact());
                continue;
              } else if ( value instanceof XsIntVal ) {
                row.setDouble(index, ((XsIntegerVal)value).getBigInteger().intValueExact());
                continue;
              } else if ( value instanceof XsIntegerVal) {
                row.setDouble(index, ((XsIntegerVal)value).getBigInteger().intValueExact());
                continue;
              } else if ( value instanceof XsLongVal) {
                row.setDouble(index, ((XsIntegerVal)value).getBigInteger().longValueExact());
                continue;
              } else if ( value instanceof XsFloatVal) {
                row.setDouble(index, ((XsFloatVal)value).getFloat());
                continue;
              } else if ( value instanceof XsDoubleVal) {
                row.setDouble(index, ((XsDoubleVal) value).getDouble());
                continue;
              }
            } else if ( column.type == Type.INTEGER ) {
              if ( value instanceof XsShortVal ) {
                row.setInteger(index, ((XsShortVal)value).getBigInteger().shortValueExact());
                continue;
              } else if ( value instanceof XsIntVal ) {
                row.setInteger(index, ((XsIntegerVal)value).getBigInteger().intValueExact());
                continue;
              } else if ( value instanceof XsIntegerVal) {
                row.setInteger(index, ((XsIntegerVal)value).getBigInteger().intValueExact());
                continue;
              } else if ( value instanceof XsLongVal) {
                row.setLongInteger(index, ((XsIntegerVal)value).getBigInteger().longValueExact());
                continue;
              }
            }
            // we already validated the column types when we added them, so the value
            // types must not be matching
            throw new IllegalStateException("Column \"" + column.columnName + "\" type " +
                column.type + " is incompatible with data type \"" + value.getClass().getName() + "\"");
          }
        }
        synchronized (this) {
          table.insert(row);
        }
      } finally {
        row.close();
      }
    } catch (TableauException e) {
      throw new MarkLogicIOException(e);
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
      synchronized(this) {
        if ( initialized == false ) {
          try {
            if ( columns.size() == 0 ) {
              for (String columnName : vals.keySet()) {
                XsAnyAtomicTypeVal value = vals.get(columnName);
                if (value instanceof XsBooleanVal) {
                  withColumn(columnName, Type.BOOLEAN);
                } else if (value instanceof XsStringVal) {
                  withColumn(columnName, Type.UNICODE_STRING);
                } else if (value instanceof XsIntegerVal ||
                    value instanceof XsIntVal ||
                    value instanceof XsShortVal ||
                    value instanceof XsLongVal ) {
                  withColumn(columnName, Type.INTEGER);
                } else if (value instanceof XsFloatVal ||
                    value instanceof XsDoubleVal ) {
                  withColumn(columnName, Type.DOUBLE);
                } else {
                  throw new IllegalStateException("Unsupported type: " + value.getClass().getName());
                }
              }
            }
            for ( Column column : columns.values() ) {
              if ( column.collation != null ) {
                tableDef.addColumnWithCollation(column.columnName,
                    column.type, column.collation);
              } else {
                tableDef.addColumn(column.columnName, column.type);
              }
            }
            // as of version 10.3.7 Tableau Data Extact SDK only supports
            // one table named "Extract"
            table = extract.addTable("Extract", tableDef);
            initialized = true;
          } catch (TableauException e) {
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
  public WriteRowToTableauConsumer withColumn(String columnName, Type type) {
    checkSupportedTypes(type);
    columns.put(columnName, new Column(columnName, type));
    return this;
  }

  /**
   * Add a column with tableau collation to the Consumer to add it in the
   * Tableau Data Extract file
   *
   * @param columnName the column name specified in the Template Driven
   *          Extraction template (the column names are case sensitive)
   * @param type the Tableau Data Type
   * @param collation the Tableau Data Collation
   * @return the Consumer for chaining
   */
  public WriteRowToTableauConsumer withColumn(String columnName, Type type, Collation collation) {
    checkSupportedTypes(type);
    columns.put(columnName, new Column(columnName, type, collation));
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
    extract.close();
  }

  private void checkSupportedTypes(Type type) {
    if ( !type.equals(Type.BOOLEAN) &&
        !type.equals(Type.DOUBLE) &&
        !type.equals(Type.INTEGER) &&
        !type.equals(Type.UNICODE_STRING) ) {
      throw new IllegalStateException("Type " + type + " is not yet supported");
    }
  }

  private class Column {
    String columnName;
    Type type;
    Collation collation;

    Column(String columnName, Type type) {
      this.columnName = columnName;
      this.type = type;
    }

    Column(String columnName, Type type, Collation collation) {
      this(columnName, type);
      this.collation = collation;
    }
  }
}
