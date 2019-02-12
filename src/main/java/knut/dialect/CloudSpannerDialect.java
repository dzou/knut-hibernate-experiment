package knut.dialect;

/** Cloud Spanner Hibernate 5.4 Dialect. */
public class CloudSpannerDialect extends AbstractCloudSpannerDialect {
  @Override
  AbstractCloudSpannerTableExporter createTableExporter(AbstractCloudSpannerDialect dialect) {
    return new CloudSpannerTableExporter(dialect);
  }

  @Override
  AbstractCloudSpannerUniqueDelegate createUniqueDelegate(AbstractCloudSpannerDialect dialect) {
    return new CloudSpannerUniqueDelegate(dialect);
  }
}
