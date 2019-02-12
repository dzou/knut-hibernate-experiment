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

import org.hibernate.boot.Metadata;
import org.hibernate.boot.internal.MetadataImpl;
import org.hibernate.boot.model.relational.Database;
import org.hibernate.tool.schema.internal.SchemaCreatorImpl;
import org.hibernate.tool.schema.spi.ExecutionOptions;
import org.hibernate.tool.schema.spi.SchemaFilter;
import org.hibernate.tool.schema.spi.SourceDescriptor;
import org.hibernate.tool.schema.spi.TargetDescriptor;

abstract class AbstractCloudSpannerSchemaCreator extends SchemaCreatorImpl {
  private final AbstractCloudSpannerSchemaManagementTool tool;

  AbstractCloudSpannerSchemaCreator(AbstractCloudSpannerSchemaManagementTool tool,
      SchemaFilter schemaFilter) {
    super(tool, schemaFilter);
    this.tool = tool;
  }

  @Override
  public void doCreation(Metadata metadata, ExecutionOptions options,
      SourceDescriptor sourceDescriptor, TargetDescriptor targetDescriptor) {
    if (metadata instanceof MetadataImpl) {
      Database original = tool.injectCloudSpannerDatabase((MetadataImpl) metadata, true);
      super.doCreation(metadata, options, sourceDescriptor, targetDescriptor);
      tool.resetMetadataDatabase((MetadataImpl) metadata, original);
    } else {
      super.doCreation(metadata, options, sourceDescriptor, targetDescriptor);
    }
  }

}
