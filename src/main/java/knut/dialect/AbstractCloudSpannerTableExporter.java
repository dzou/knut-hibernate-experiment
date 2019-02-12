/*
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package knut.dialect;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.cloud.spanner.jdbc.CloudSpannerJdbcConnection;
import org.hibernate.HibernateException;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.boot.model.relational.InitCommand;
import org.hibernate.boot.model.relational.QualifiedTableName;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.Mapping;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.Constraint;
import org.hibernate.mapping.ForeignKey;
import org.hibernate.mapping.Index;
import org.hibernate.mapping.KeyValue;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.PrimaryKey;
import org.hibernate.mapping.Table;
import org.hibernate.mapping.UniqueKey;
import org.hibernate.tool.hbm2ddl.TableMetadata;
import org.hibernate.tool.schema.internal.StandardTableExporter;

// dzou@: This is a reference to his specific JDBC driver in the Hibernate Dialect.
// If we can come up with a substitute, we can decouple the JDBC driver from the Dialect.
// import com.google.cloud.spanner.jdbc.CloudSpannerJdbcConnection;

@SuppressWarnings("deprecation")
public abstract class AbstractCloudSpannerTableExporter extends StandardTableExporter {
  private static final class ColumnComparator implements Comparator<Column> {
    private final Table table;
    private final Table parentTable;
    private final Metadata metadata;

    private ColumnComparator(Table table, Table parentTable, Metadata metadata) {
      this.table = table;
      this.parentTable = parentTable;
      this.metadata = metadata;
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compare(Column o1, Column o2) {
      Iterator<Column> it = parentTable.getColumnIterator();
      boolean o1InParent = false;
      boolean o2InParent = false;
      while (it.hasNext()) {
        Column parentColumn = it.next();
        o1InParent = o1InParent || o1.getName().equalsIgnoreCase(parentColumn.getName());
        o2InParent = o2InParent || o2.getName().equalsIgnoreCase(parentColumn.getName());
      }
      boolean o1InPK = table.getPrimaryKey().containsColumn(o1);
      boolean o2InPK = table.getPrimaryKey().containsColumn(o2);
      if (!o1InPK && !o2InPK) {
        return 0;
      } else if (o1InPK && !o2InPK) {
        return -1;
      } else if (!o1InPK && o2InPK) {
        return 1;
      } else if (o1InPK && o2InPK) {
        if (o1InParent && !o2InParent) {
          return -1;
        } else if (!o1InParent && o2InParent) {
          return 1;
        }
      }
      InterleaveInParent interleave = getInterleaveAnnotation(metadata, parentTable);
      if (interleave != null) {
        Table newParentTable = getTable(metadata, interleave.value());
        ColumnComparator comp = new ColumnComparator(parentTable, newParentTable, metadata);
        return comp.compare(o1, o2);
      }
      return 0;
    }
  }

  private static final class CloudSpannerPrimaryKeyWithParent extends PrimaryKey {
    private static final long serialVersionUID = 1L;
    private final PrimaryKey delegate;
    private final Table parentTable;
    private final Metadata metadata;

    private CloudSpannerPrimaryKeyWithParent(PrimaryKey delegate, Table parentTable,
        Metadata metadata) {
      super(delegate.getTable());
      this.delegate = delegate;
      this.parentTable = parentTable;
      this.metadata = metadata;
    }

    @Override
    public Iterator<Column> getColumnIterator() {
      List<Column> list = new ArrayList<>();
      Iterator<Column> it = delegate.getColumnIterator();
      while (it.hasNext()) {
        list.add(it.next());
      }
      Collections.sort(list, new ColumnComparator(delegate.getTable(), parentTable, metadata));
      return list.iterator();
    }

    @Override
    public Iterator<Column> columnIterator() {
      return this.getColumnIterator();
    }

    @Override
    public void addColumn(Column column) {
      delegate.addColumn(column);
    }

    @Override
    public String getName() {
      return delegate.getName();
    }

    @Override
    public void setName(String name) {
      delegate.setName(name);
    }

    @Override
    public int hashCode() {
      return delegate.hashCode();
    }

    @Override
    public String generatedConstraintNamePrefix() {
      return delegate.generatedConstraintNamePrefix();
    }

    @Override
    public String getExportIdentifier() {
      return delegate.getExportIdentifier();
    }

    @Override
    public boolean equals(Object obj) {
      return delegate.equals(obj);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void addColumns(Iterator columnIterator) {
      delegate.addColumns(columnIterator);
    }

    @Override
    public boolean containsColumn(Column column) {
      return delegate.containsColumn(column);
    }

    @Override
    public int getColumnSpan() {
      return delegate.getColumnSpan();
    }

    @Override
    public Column getColumn(int i) {
      return delegate.getColumn(i);
    }

    @Override
    public Table getTable() {
      return delegate.getTable();
    }

    @Override
    public void setTable(Table table) {
      if (delegate != null) {
        delegate.setTable(table);
      }
    }

    @Override
    public boolean isGenerated(Dialect dialect) {
      return delegate.isGenerated(dialect);
    }

    @Override
    public String sqlDropString(Dialect dialect, String defaultCatalog, String defaultSchema) {
      return delegate.sqlDropString(dialect, defaultCatalog, defaultSchema);
    }

    @Override
    public List<Column> getColumns() {
      return delegate.getColumns();
    }

    @Override
    public String toString() {
      return delegate.toString();
    }
  }

  abstract static class AbstractCloudSpannerTableWithParent extends Table {
    private static final long serialVersionUID = 1L;
    private final Table delegate;
    private final Table parentTable;
    private final Metadata metadata;

    AbstractCloudSpannerTableWithParent(Table delegate, Table parentTable, Metadata metadata) {
      this.delegate = delegate;
      this.parentTable = parentTable;
      this.metadata = metadata;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<Column> getColumnIterator() {
      List<Column> list = new ArrayList<>();
      Iterator<Column> it = delegate.getColumnIterator();
      while (it.hasNext()) {
        list.add(it.next());
      }
      Collections.sort(list, new ColumnComparator(delegate, parentTable, metadata));
      return list.iterator();
    }

    @Override
    public PrimaryKey getPrimaryKey() {
      return new CloudSpannerPrimaryKeyWithParent(delegate.getPrimaryKey(), parentTable, metadata);
    }

    @Override
    public String getQualifiedName(Dialect dialect, String defaultCatalog, String defaultSchema) {
      return delegate.getQualifiedName(dialect, defaultCatalog, defaultSchema);
    }

    @Override
    public void setName(String name) {
      delegate.setName(name);
    }

    @Override
    public String getName() {
      return delegate.getName();
    }

    @Override
    public Identifier getNameIdentifier() {
      return delegate.getNameIdentifier();
    }

    @Override
    public String getQuotedName() {
      return delegate.getQuotedName();
    }

    @Override
    public String getQuotedName(Dialect dialect) {
      return delegate.getQuotedName(dialect);
    }

    @Override
    public QualifiedTableName getQualifiedTableName() {
      return delegate.getQualifiedTableName();
    }

    @Override
    public boolean isQuoted() {
      return delegate.isQuoted();
    }

    @Override
    public void setQuoted(boolean quoted) {
      delegate.setQuoted(quoted);
    }

    @Override
    public void setSchema(String schema) {
      delegate.setSchema(schema);
    }

    @Override
    public String getSchema() {
      return delegate.getSchema();
    }

    @Override
    public String getQuotedSchema() {
      return delegate.getQuotedSchema();
    }

    @Override
    public String getQuotedSchema(Dialect dialect) {
      return delegate.getQuotedSchema(dialect);
    }

    @Override
    public boolean isSchemaQuoted() {
      return delegate.isSchemaQuoted();
    }

    @Override
    public void setCatalog(String catalog) {
      delegate.setCatalog(catalog);
    }

    @Override
    public String getCatalog() {
      return delegate.getCatalog();
    }

    @Override
    public String getQuotedCatalog() {
      return delegate.getQuotedCatalog();
    }

    @Override
    public String getQuotedCatalog(Dialect dialect) {
      return delegate.getQuotedCatalog(dialect);
    }

    @Override
    public boolean isCatalogQuoted() {
      return delegate.isCatalogQuoted();
    }

    @Override
    public Column getColumn(Column column) {
      return delegate.getColumn(column);
    }

    @Override
    public Column getColumn(Identifier name) {
      return delegate.getColumn(name);
    }

    @Override
    public Column getColumn(int n) {
      return delegate.getColumn(n);
    }

    @Override
    public void addColumn(Column column) {
      delegate.addColumn(column);
    }

    @Override
    public int getColumnSpan() {
      return delegate.getColumnSpan();
    }

    @Override
    public Iterator<Index> getIndexIterator() {
      return delegate.getIndexIterator();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Iterator getForeignKeyIterator() {
      return delegate.getForeignKeyIterator();
    }

    @Override
    public Map<ForeignKeyKey, ForeignKey> getForeignKeys() {
      return delegate.getForeignKeys();
    }

    @Override
    public Iterator<UniqueKey> getUniqueKeyIterator() {
      return delegate.getUniqueKeyIterator();
    }

    @Override
    public int hashCode() {
      return delegate.hashCode();
    }

    @Override
    public boolean equals(Object object) {
      return delegate.equals(object);
    }

    @Override
    public boolean equals(Table table) {
      return delegate.equals(table);
    }

    @Override
    public void validateColumns(Dialect dialect, Mapping mapping, TableMetadata tableInfo) {
      delegate.validateColumns(dialect, mapping, tableInfo);
    }

    @Override
    public boolean hasPrimaryKey() {
      return delegate.hasPrimaryKey();
    }

    @Override
    public String sqlCreateString(Dialect dialect, Mapping p, String defaultCatalog,
        String defaultSchema) {
      return delegate.sqlCreateString(dialect, p, defaultCatalog, defaultSchema);
    }

    @Override
    public String sqlDropString(Dialect dialect, String defaultCatalog, String defaultSchema) {
      return delegate.sqlDropString(dialect, defaultCatalog, defaultSchema);
    }

    @Override
    public void setPrimaryKey(PrimaryKey primaryKey) {
      delegate.setPrimaryKey(primaryKey);
    }

    @Override
    public Index getOrCreateIndex(String indexName) {
      return delegate.getOrCreateIndex(indexName);
    }

    @Override
    public Index getIndex(String indexName) {
      return delegate.getIndex(indexName);
    }

    @Override
    public Index addIndex(Index index) {
      return delegate.addIndex(index);
    }

    @Override
    public UniqueKey addUniqueKey(UniqueKey uniqueKey) {
      return delegate.addUniqueKey(uniqueKey);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public UniqueKey createUniqueKey(List keyColumns) {
      return delegate.createUniqueKey(keyColumns);
    }

    @Override
    public UniqueKey getUniqueKey(String keyName) {
      return delegate.getUniqueKey(keyName);
    }

    @Override
    public UniqueKey getOrCreateUniqueKey(String keyName) {
      return delegate.getOrCreateUniqueKey(keyName);
    }

    @Override
    public void createForeignKeys() {
      delegate.createForeignKeys();
    }

    @Override
    public void setUniqueInteger(int uniqueInteger) {
      delegate.setUniqueInteger(uniqueInteger);
    }

    @Override
    public int getUniqueInteger() {
      return delegate.getUniqueInteger();
    }

    @Override
    public void setIdentifierValue(KeyValue idValue) {
      delegate.setIdentifierValue(idValue);
    }

    @Override
    public KeyValue getIdentifierValue() {
      return delegate.getIdentifierValue();
    }

    @Override
    public void addCheckConstraint(String constraint) {
      delegate.addCheckConstraint(constraint);
    }

    @Override
    public boolean containsColumn(Column column) {
      return delegate.containsColumn(column);
    }

    @Override
    public String getRowId() {
      return delegate.getRowId();
    }

    @Override
    public void setRowId(String rowId) {
      delegate.setRowId(rowId);
    }

    @Override
    public String toString() {
      return delegate.toString();
    }

    @Override
    public String getSubselect() {
      return delegate.getSubselect();
    }

    @Override
    public void setSubselect(String subselect) {
      delegate.setSubselect(subselect);
    }

    @Override
    public boolean isSubselect() {
      return delegate.isSubselect();
    }

    @Override
    public boolean isAbstractUnionTable() {
      return delegate.isAbstractUnionTable();
    }

    @Override
    public boolean hasDenormalizedTables() {
      return delegate.hasDenormalizedTables();
    }

    @Override
    public void setAbstract(boolean isAbstract) {
      delegate.setAbstract(isAbstract);
    }

    @Override
    public boolean isAbstract() {
      return delegate.isAbstract();
    }

    @Override
    public boolean isPhysicalTable() {
      return delegate.isPhysicalTable();
    }

    @Override
    public String getComment() {
      return delegate.getComment();
    }

    @Override
    public void setComment(String comment) {
      delegate.setComment(comment);
    }

    @Override
    public Iterator<String> getCheckConstraintsIterator() {
      return delegate.getCheckConstraintsIterator();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Iterator sqlCommentStrings(Dialect dialect, String defaultCatalog,
        String defaultSchema) {
      return delegate.sqlCommentStrings(dialect, defaultCatalog, defaultSchema);
    }

    @Override
    public String getExportIdentifier() {
      return delegate.getExportIdentifier();
    }

    @Override
    public void addInitCommand(InitCommand command) {
      delegate.addInitCommand(command);
    }

    @Override
    public List<InitCommand> getInitCommands() {
      return delegate.getInitCommands();
    }
  }

  private final AbstractCloudSpannerDialect dialect;

  public AbstractCloudSpannerTableExporter(AbstractCloudSpannerDialect dialect) {
    super(dialect);
    this.dialect = dialect;
  }

  abstract AbstractCloudSpannerTableWithParent createTableWithParent(Table table, Table parentTable,
      Metadata metadata);

  /**
   * Cloud Spanner has a couple of differences with other database systems that we need to take into
   * account when we generate a new table:
   * <ol>
   * <li>The PRIMARY KEY specification must be added outside the column creation, and cannot be
   * added as a CONSTRAINT after the list of columns.</li>
   * <li>If the table is interleaved in another table, we need to add this to the end of the
   * creation string. Interleaving is specified using the {@link InterleaveInParent}
   * annotation.</li>
   * <li>If the table is interleaved in another table, we need to make sure that the order in which
   * we specify the primary key columns is correct and consistent with the parent table.</li>
   * </ol>
   */
  @Override
  public String[] getSqlCreateStrings(Table table, Metadata metadata) {
    InterleaveInParent interleave = getInterleaveAnnotation(metadata, table);
    if (interleave != null) {
      Table parentTable = getTable(metadata, interleave.value());
      if (parentTable == null) {
        throw new HibernateException(
            "Parent table " + interleave.value() + " of table " + table.getName() + " not found");
      }
      table = createTableWithParent(table, parentTable, metadata);
    }
    String[] res = super.getSqlCreateStrings(table, metadata);
    String create = res[0];
    // move the primary key definition to the end
    if (table.hasPrimaryKey()) {
      String pkDef = ", " + table.getPrimaryKey().sqlConstraintString(dialect);
      create = create.replace(pkDef, "");
      // then append it to the end of the create string
      create = create + " " + table.getPrimaryKey().sqlConstraintString(dialect);
      // check if we need to add an INTERLEAVE IN PARENT clause
      if (interleave != null) {
        create = create + ", INTERLEAVE IN PARENT " + dialect.quote(interleave.value());
        if (interleave.cascadeDelete()) {
          create = create + " ON DELETE CASCADE";
        }
      }
      // return the new create string
      res[0] = create;
    } else {
      throw new HibernateException("Table " + table.getName() + " does not specify a primary key");
    }
    return res;
  }

  private static InterleaveInParent getInterleaveAnnotation(Metadata metadata, Table table) {
    for (PersistentClass pc : metadata.getEntityBindings()) {
      if (pc.getTable().equals(table)) {
        Class<?> entityClass = pc.getMappedClass();
        return entityClass.getAnnotation(InterleaveInParent.class);
      }
    }
    return null;
  }

  private static Table getTable(Metadata metadata, String name) {
    for (Table t : metadata.collectTableMappings()) {
      if (t.getName().equalsIgnoreCase(name)) {
        return t;
      }
    }
    return null;
  }

  @Override
  public String[] getSqlDropStrings(Table table, Metadata metadata) {
    // Check for actually existing table and indices.
    if (!tableExists(table))
      return new String[] {};
    Set<String> existingIndices = getIndicesExcludingPK(table);

    if (existingIndices.isEmpty())
      return super.getSqlDropStrings(table, metadata);

    List<String> dropIndices = new ArrayList<>();
    for (String index : existingIndices) {
      dropIndices.add("DROP INDEX `" + index + "`");
    }
    String[] tableDrop = super.getSqlDropStrings(table, metadata);

    String[] res = new String[dropIndices.size() + tableDrop.length];
    dropIndices.toArray(res);
    System.arraycopy(tableDrop, 0, res, dropIndices.size(), tableDrop.length);

    return res;
  }

  private boolean tableExists(Table table) {
    if (dialect.getMetadata() == null)
      return false;
    boolean exists = true;
    try {
      if (dialect.getMetadata().getConnection().isWrapperFor(CloudSpannerJdbcConnection.class)) {
        CloudSpannerJdbcConnection existingConnection =
            dialect.getMetadata().getConnection().unwrap(CloudSpannerJdbcConnection.class);
        try (Connection connection =
            DriverManager.getConnection(existingConnection.getConnectionUrl())) {
          connection.setAutoCommit(true);
          try (ResultSet tables = connection.getMetaData().getTables(table.getCatalog(),
              table.getSchema(), table.getName(), null)) {
            exists = tables.next();
          }
        }
      }
    } catch (SQLException e) {
      // ignore at this point, just try to drop it.
    }
    return exists;
  }

  private Set<String> getIndicesExcludingPK(Table table) {
    Set<String> res = new HashSet<>();
    Iterator<Index> indexIterator = table.getIndexIterator();
    while (indexIterator.hasNext()) {
      Index index = indexIterator.next();
      if (indexExists(index.getName())) {
        res.add(index.getName());
      }
    }
    Iterator<UniqueKey> keyIterator = table.getUniqueKeyIterator();
    while (keyIterator.hasNext()) {
      UniqueKey key = keyIterator.next();
      if (indexExists(key.getName())) {
        res.add(key.getName());
      }
    }
    @SuppressWarnings("unchecked")
    Iterator<Column> colIterator = table.getColumnIterator();
    while (colIterator.hasNext()) {
      Column col = colIterator.next();
      if (col.isUnique()) {
        String name = Constraint.generateName("UK_", table, col);
        if (indexExists(name)) {
          res.add(name);
        }
      }
    }
    return res;
  }

  private boolean indexExists(String name) {
    if (dialect.getMetadata() == null)
      return false;
    boolean exists = true;
    try {
      if (dialect.getMetadata().getConnection().isWrapperFor(CloudSpannerJdbcConnection.class)) {
        CloudSpannerJdbcConnection existingConnection =
            dialect.getMetadata().getConnection().unwrap(CloudSpannerJdbcConnection.class);
        try (Connection connection =
            DriverManager.getConnection(existingConnection.getConnectionUrl())) {
          connection.setAutoCommit(true);
          try (PreparedStatement ps = connection.prepareStatement(
              "SELECT INDEX_NAME FROM INFORMATION_SCHEMA.INDEXES WHERE INDEX_NAME=?")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
              exists = rs.next();
            }
          }
        }
      }
    } catch (SQLException e) {
      // ignore at this point, just try to drop it.
    }
    return exists;
  }

}
