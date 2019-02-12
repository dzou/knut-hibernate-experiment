package knut.dialect;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.internal.MetadataImpl;
import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.boot.model.relational.Database;
import org.hibernate.boot.model.relational.Namespace;
import org.hibernate.boot.model.relational.Sequence;
import org.hibernate.mapping.DenormalizedTable;
import org.hibernate.mapping.Table;
import org.hibernate.tool.schema.spi.SchemaCreator;
import org.hibernate.tool.schema.spi.SchemaDropper;

public class CloudSpannerSchemaManagementTool extends AbstractCloudSpannerSchemaManagementTool {
  private static final long serialVersionUID = 1L;

  static class CloudSpannerDatabase5_4 extends CloudSpannerDatabase {
    CloudSpannerDatabase5_4(MetadataImpl metadata, Database delegate, boolean parentTablesFirst) {
      super(metadata, delegate, parentTablesFirst);
    }

    @Override
    Namespace createNamespaceWithOrderedTables(MetadataImpl metadata, Database database,
        Namespace namespace, boolean parentTablesFirst) {
      return new NamespaceWithOrderedTables5_4(metadata, database, namespace, parentTablesFirst);
    }
  }

  private static class NamespaceWithOrderedTables5_4 extends Namespace {
    private final TableComparator comparator;
    private final Namespace delegate;

    NamespaceWithOrderedTables5_4(Metadata metadata, Database database, Namespace delegate,
        boolean parentTablesFirst) {
      super(database.getPhysicalNamingStrategy(), database.getJdbcEnvironment(),
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

  @Override
  CloudSpannerDatabase createTemporaryDatabase(MetadataImpl metadata, Database original,
      boolean parentTablesFirst) {
    return new CloudSpannerDatabase5_4(metadata, original, parentTablesFirst);
  }

  @Override
  public SchemaCreator getSchemaCreator(@SuppressWarnings("rawtypes") Map options) {
    return new CloudSpannerSchemaCreator(this, getSchemaFilterProvider(options).getCreateFilter());
  }

  @Override
  public SchemaDropper getSchemaDropper(@SuppressWarnings("rawtypes") Map options) {
    return new CloudSpannerSchemaDropper(this, getSchemaFilterProvider(options).getDropFilter());
  }
}
