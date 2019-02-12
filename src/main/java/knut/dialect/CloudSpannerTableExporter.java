package knut.dialect;

import org.hibernate.boot.Metadata;
import org.hibernate.mapping.Table;

class CloudSpannerTableExporter extends AbstractCloudSpannerTableExporter {
  static class CloudSpannerTableWithParent extends AbstractCloudSpannerTableWithParent {
    private static final long serialVersionUID = 1L;

    CloudSpannerTableWithParent(Table delegate, Table parentTable, Metadata metadata) {
      super(delegate, parentTable, metadata);
    }
  }

  CloudSpannerTableExporter(AbstractCloudSpannerDialect dialect) {
    super(dialect);
  }

  @Override
  AbstractCloudSpannerTableWithParent createTableWithParent(Table table, Table parentTable,
      Metadata metadata) {
    return new CloudSpannerTableWithParent(table, parentTable, metadata);
  }
}
