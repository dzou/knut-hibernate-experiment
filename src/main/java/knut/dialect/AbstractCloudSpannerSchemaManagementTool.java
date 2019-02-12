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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.hibernate.HibernateException;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.internal.MetadataImpl;
import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.boot.model.relational.AuxiliaryDatabaseObject;
import org.hibernate.boot.model.relational.Database;
import org.hibernate.boot.model.relational.Namespace;
import org.hibernate.boot.model.relational.Sequence;
import org.hibernate.boot.registry.selector.spi.StrategySelector;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.dialect.Dialect;
import org.hibernate.mapping.DenormalizedTable;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.Table;
import org.hibernate.tool.schema.JdbcMetadaAccessStrategy;
import org.hibernate.tool.schema.internal.DefaultSchemaFilterProvider;
import org.hibernate.tool.schema.internal.GroupedSchemaMigratorImpl;
import org.hibernate.tool.schema.internal.HibernateSchemaManagementTool;
import org.hibernate.tool.schema.internal.IndividuallySchemaMigratorImpl;
import org.hibernate.tool.schema.spi.SchemaCreator;
import org.hibernate.tool.schema.spi.SchemaDropper;
import org.hibernate.tool.schema.spi.SchemaFilterProvider;
import org.hibernate.tool.schema.spi.SchemaManagementTool;
import org.hibernate.tool.schema.spi.SchemaMigrator;

/**
 * {@link SchemaManagementTool} for Cloud Spanner. This tool ensures that tables are created and
 * dropped in the correct order. Normally Hibernate would try to create/drop tables in alphabetical
 * order, but that does not work for Cloud Spanner if the tables are interleaved in each other. This
 * class ensures that the tables are ordered correctly.
 *
 * The tool also batches the DDL statements together in one DDL batch. This makes generating a new
 * schema a lot faster.
 *
 * You <strong>must</strong> specify this {@link SchemaManagementTool} in your configuration if you
 * want Hibernate to generate your schema. If you configure Hibernate using a hibernate.cfg.xml
 * file, you should include the following property:
 *
 * <pre>
 * &lt;property name=
"schema_management_tool"&gt;com.google.cloud.spanner.hibernate.CloudSpannerSchemaManagementTool&lt;/property&gt;
 * </pre>
 */
abstract class AbstractCloudSpannerSchemaManagementTool extends HibernateSchemaManagementTool {
  private static final long serialVersionUID = 1L;

  static class CloudSpannerDatabase extends Database {
    private final MetadataImpl metadata;
    private final Database delegate;
    private final boolean parentTablesFirst;

    CloudSpannerDatabase(MetadataImpl metadata, Database delegate, boolean parentTablesFirst) {
      super(metadata.getMetadataBuildingOptions());
      this.metadata = metadata;
      this.delegate = delegate;
      this.parentTablesFirst = parentTablesFirst;
    }

    Namespace createNamespaceWithOrderedTables(MetadataImpl metadata, Database database,
        Namespace namespace, boolean parentTablesFirst) {
      return new NamespaceWithOrderedTables(metadata, database, namespace, parentTablesFirst);
    }

    @Override
    public Iterable<Namespace> getNamespaces() {
      Iterable<Namespace> namespaces = delegate.getNamespaces();
      List<Namespace> res = new ArrayList<>();
      Iterator<Namespace> it = namespaces.iterator();
      while (it.hasNext()) {
        res.add(createNamespaceWithOrderedTables(metadata, delegate, it.next(), parentTablesFirst));
      }
      return res;
    }

    @Override
    public Collection<AuxiliaryDatabaseObject> getAuxiliaryDatabaseObjects() {
      Collection<AuxiliaryDatabaseObject> collection = super.getAuxiliaryDatabaseObjects();
      List<AuxiliaryDatabaseObject> list = new ArrayList<>(collection);
      list.sort(new Comparator<AuxiliaryDatabaseObject>() {
        @Override
        public int compare(AuxiliaryDatabaseObject o1, AuxiliaryDatabaseObject o2) {
          if (o1 instanceof StartBatchDdl) {
            return -1;
          }
          if (o1 instanceof RunBatchDdl) {
            return 1;
          }
          if (o2 instanceof StartBatchDdl) {
            return 1;
          }
          if (o2 instanceof RunBatchDdl) {
            return -1;
          }
          return 0;
        }
      });
      return list;
    }
  }

  static class NamespaceWithOrderedTables extends Namespace {
    private final TableComparator comparator;
    private final Namespace delegate;

    NamespaceWithOrderedTables(Metadata metadata, Database database, Namespace delegate,
        boolean parentTablesFirst) {
      super(database.getPhysicalNamingStrategy(),
          database.getJdbcEnvironment(),
          delegate.getName());
      this.comparator = new TableComparator(metadata, delegate, parentTablesFirst);
      this.delegate = delegate;
    }

    @Override
    public Collection<Table> getTables() {
      Collection<Table> tables = delegate.getTables();
      List<Table> res = new ArrayList<>(tables);
      // order the tables by putting parent tables before child tables
      res.sort(comparator);
      return res;
    }

    @Override
    public Name getName() {
      return delegate.getName();
    }

    @Override
    public Name getPhysicalName() {
      return delegate.getPhysicalName();
    }

    @Override
    public Table locateTable(Identifier logicalTableName) {
      return delegate.locateTable(logicalTableName);
    }

    @Override
    public Table createTable(Identifier logicalTableName, boolean isAbstract) {
      return delegate.createTable(logicalTableName, isAbstract);
    }

    @Override
    public DenormalizedTable createDenormalizedTable(Identifier logicalTableName,
        boolean isAbstract, Table includedTable) {
      return delegate.createDenormalizedTable(logicalTableName, isAbstract, includedTable);
    }

    @Override
    public Sequence locateSequence(Identifier name) {
      return delegate.locateSequence(name);
    }

    @Override
    public Sequence createSequence(Identifier logicalName, int initialValue, int increment) {
      return delegate.createSequence(logicalName, initialValue, increment);
    }

    @Override
    public String toString() {
      return delegate.toString();
    }

    @Override
    public boolean equals(Object o) {
      return delegate.equals(o);
    }

    @Override
    public int hashCode() {
      return delegate.hashCode();
    }

    @Override
    public Iterable<Sequence> getSequences() {
      return delegate.getSequences();
    }
  }

  static class TableComparator implements Comparator<Table> {
    private final Metadata metadata;
    private final Namespace namespace;
    private final boolean parentTablesFirst;

    TableComparator(Metadata metadata, Namespace namespace, boolean parentTablesFirst) {
      this.metadata = metadata;
      this.namespace = namespace;
      this.parentTablesFirst = parentTablesFirst;
    }

    @Override
    public int compare(Table o1, Table o2) {
      int res = compareTables(o1, o2);
      return parentTablesFirst ? res : res * -1;
    }

    int compareTables(Table table1, Table table2) {
      InterleaveInParent interleaveTable1 = getInterleaveAnnotation(table1);
      InterleaveInParent interleaveTable2 = getInterleaveAnnotation(table2);
      if (interleaveTable1 == null && interleaveTable2 != null) {
        return -1;
      } else if (interleaveTable1 != null && interleaveTable2 == null) {
        return 1;
      } else if (interleaveTable1 == null && interleaveTable2 == null) {
        return 0;
      } else if (interleaveTable1 != null && interleaveTable2 != null) {
        if (isParentOf(table1, table2)) {
          return -1;
        }
        if (isParentOf(table2, table1)) {
          return 1;
        }
      }
      return 0;
    }

    private boolean isParentOf(Table potentialParent, Table potentialChild) {
      InterleaveInParent interleave = getInterleaveAnnotation(potentialChild);
      while (interleave != null) {
        String parent = interleave.value();
        if (parent.equalsIgnoreCase(potentialParent.getName())) {
          return true;
        }
        potentialChild = getTable(parent);
        interleave = getInterleaveAnnotation(potentialChild);
      }
      return false;
    }

    private InterleaveInParent getInterleaveAnnotation(Table table) {
      for (PersistentClass pc : metadata.getEntityBindings()) {
        if (pc.getTable().equals(table)) {
          Class<?> entityClass = pc.getMappedClass();
          return entityClass.getAnnotation(InterleaveInParent.class);
        }
      }
      return null;
    }

    private Table getTable(String name) {
      for (Table t : namespace.getTables()) {
        if (t.getName().equalsIgnoreCase(name)) {
          return t;
        }
      }
      return null;
    }
  }

  private static final class StartBatchDdl implements AuxiliaryDatabaseObject {
    private static final long serialVersionUID = 1L;

    @Override
    public String getExportIdentifier() {
      return "START_BATCH_DDL";
    }

    @Override
    public boolean appliesToDialect(Dialect dialect) {
      return AbstractCloudSpannerDialect.class.isAssignableFrom(dialect.getClass());
    }

    @Override
    public boolean beforeTablesOnCreation() {
      return true;
    }

    @Override
    public String[] sqlCreateStrings(Dialect dialect) {
      return new String[] {"START BATCH DDL"};
    }

    @Override
    public String[] sqlDropStrings(Dialect dialect) {
      return new String[] {"START BATCH DDL"};
    }
  }

  private static final class RunBatchDdl implements AuxiliaryDatabaseObject {
    private static final long serialVersionUID = 1L;

    @Override
    public String getExportIdentifier() {
      return "RUN_BATCH_DDL";
    }

    @Override
    public boolean appliesToDialect(Dialect dialect) {
      return AbstractCloudSpannerDialect.class.isAssignableFrom(dialect.getClass());
    }

    @Override
    public boolean beforeTablesOnCreation() {
      return false;
    }

    @Override
    public String[] sqlCreateStrings(Dialect dialect) {
      return new String[] {"RUN BATCH"};
    }

    @Override
    public String[] sqlDropStrings(Dialect dialect) {
      return new String[] {"RUN BATCH"};
    }
  }

  CloudSpannerDatabase createTemporaryDatabase(MetadataImpl metadata, Database original,
      boolean parentTablesFirst) {
    return new CloudSpannerDatabase(metadata, original, parentTablesFirst);
  }

  Database injectCloudSpannerDatabase(MetadataImpl metadata, boolean parentTablesFirst) {
    Database original = metadata.getDatabase();
    CloudSpannerDatabase temporaryDatabase =
        createTemporaryDatabase(metadata, original, parentTablesFirst);
    temporaryDatabase.addAuxiliaryDatabaseObject(new StartBatchDdl());
    temporaryDatabase.addAuxiliaryDatabaseObject(new RunBatchDdl());
    try {
      Field field = MetadataImpl.class.getDeclaredField("database");
      field.setAccessible(true);
      field.set(metadata, temporaryDatabase);
    } catch (NoSuchFieldException | SecurityException | IllegalArgumentException
        | IllegalAccessException e) {
      throw new HibernateException("Could not inject temporary metadata database", e);
    }
    return original;
  }

  void resetMetadataDatabase(MetadataImpl metadata, Database original) {
    try {
      Field field = MetadataImpl.class.getDeclaredField("database");
      field.setAccessible(true);
      field.set(metadata, original);
    } catch (SecurityException | IllegalArgumentException | IllegalAccessException
        | NoSuchFieldException e) {
      throw new HibernateException("Could not reset metadata database", e);
    }
  }

  @Override
  public abstract SchemaCreator getSchemaCreator(@SuppressWarnings("rawtypes") Map options);

  @Override
  public abstract SchemaDropper getSchemaDropper(@SuppressWarnings("rawtypes") Map options);

  @Override
  public SchemaMigrator getSchemaMigrator(@SuppressWarnings("rawtypes") Map options) {
    if (JdbcMetadaAccessStrategy.interpretSetting(options) == JdbcMetadaAccessStrategy.GROUPED) {
      return new GroupedSchemaMigratorImpl(this,
          getSchemaFilterProvider(options).getMigrateFilter());
    } else {
      return new IndividuallySchemaMigratorImpl(this,
          getSchemaFilterProvider(options).getMigrateFilter());
    }
  }

  SchemaFilterProvider getSchemaFilterProvider(@SuppressWarnings("rawtypes") Map options) {
    final Object configuredOption =
        (options == null) ? null : options.get(AvailableSettings.HBM2DDL_FILTER_PROVIDER);
    return getServiceRegistry().getService(StrategySelector.class).resolveDefaultableStrategy(
        SchemaFilterProvider.class, configuredOption, DefaultSchemaFilterProvider.INSTANCE);
  }

}
