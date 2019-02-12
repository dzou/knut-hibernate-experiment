package knut.dialect;

import org.hibernate.tool.schema.spi.SchemaFilter;

class CloudSpannerSchemaCreator extends AbstractCloudSpannerSchemaCreator {
  CloudSpannerSchemaCreator(AbstractCloudSpannerSchemaManagementTool tool,
      SchemaFilter schemaFilter) {
    super(tool, schemaFilter);
  }
}
