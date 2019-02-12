package knut.dialect;

import org.hibernate.tool.schema.spi.SchemaFilter;

class CloudSpannerSchemaDropper extends AbstractCloudSpannerSchemaDropper {
  CloudSpannerSchemaDropper(AbstractCloudSpannerSchemaManagementTool tool,
      SchemaFilter schemaFilter) {
    super(tool, schemaFilter);
  }
}
