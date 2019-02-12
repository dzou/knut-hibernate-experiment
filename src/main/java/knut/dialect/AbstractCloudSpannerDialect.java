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

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Types;
import org.hibernate.boot.Metadata;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.function.StandardSQLFunction;
import org.hibernate.dialect.pagination.AbstractLimitHandler;
import org.hibernate.dialect.pagination.LimitHandler;
import org.hibernate.dialect.pagination.LimitHelper;
import org.hibernate.dialect.unique.UniqueDelegate;
import org.hibernate.engine.jdbc.env.spi.IdentifierHelper;
import org.hibernate.engine.jdbc.env.spi.IdentifierHelperBuilder;
import org.hibernate.engine.spi.Mapping;
import org.hibernate.engine.spi.RowSelection;
import org.hibernate.mapping.ForeignKey;
import org.hibernate.mapping.Table;
import org.hibernate.tool.schema.spi.Exporter;
import org.hibernate.type.StandardBasicTypes;
import org.hibernate.type.Type;

/** Hibernate dialect for Google Cloud Spanner */
public abstract class AbstractCloudSpannerDialect extends Dialect {
  /** Max length of STRING columns in Cloud Spanner */
  public static final int STRING_MAX_LENGTH = 2621440;
  /** Max length of BYTES columns in Cloud Spanner */
  public static final int BYTES_MAX_LENGTH = 10485760;

  private static final AbstractLimitHandler LIMIT_HANDLER = new AbstractLimitHandler() {
    @Override
    public String processSql(String sql, RowSelection selection) {
      final boolean hasOffset = LimitHelper.hasFirstRow(selection);
      return sql + (hasOffset ? " limit ? offset ?" : " limit ?");
    }

    @Override
    public boolean supportsLimit() {
      return true;
    }

    @Override
    public boolean bindLimitParametersInReverseOrder() {
      return true;
    }
  };

  private final AbstractCloudSpannerTableExporter tableExporter;
  private final UniqueDelegate uniqueDelegate;

  private DatabaseMetaData metadata;

  public AbstractCloudSpannerDialect() {
    registerColumnType(Types.BOOLEAN, "BOOL");
    registerColumnType(Types.BIT, "BOOL");
    registerColumnType(Types.BIGINT, "INT64");
    registerColumnType(Types.SMALLINT, "INT64");
    registerColumnType(Types.TINYINT, "INT64");
    registerColumnType(Types.INTEGER, "INT64");
    registerColumnType(Types.CHAR, "STRING(1)");
    registerColumnType(Types.VARCHAR, STRING_MAX_LENGTH, "STRING($l)");
    registerColumnType(Types.FLOAT, "FLOAT64");
    registerColumnType(Types.DOUBLE, "FLOAT64");
    registerColumnType(Types.DATE, "DATE");
    registerColumnType(Types.TIME, "TIMESTAMP");
    registerColumnType(Types.TIMESTAMP, "TIMESTAMP");
    registerColumnType(Types.VARBINARY, BYTES_MAX_LENGTH, "BYTES($l)");
    registerColumnType(Types.BINARY, BYTES_MAX_LENGTH, "BYTES($l)");
    registerColumnType(Types.LONGVARCHAR, STRING_MAX_LENGTH, "STRING($l)");
    registerColumnType(Types.LONGVARBINARY, BYTES_MAX_LENGTH, "BYTES($l)");
    registerColumnType(Types.CLOB, "STRING(MAX)");
    registerColumnType(Types.NCLOB, "STRING(MAX)");
    registerColumnType(Types.BLOB, "BYTES(MAX)");
    // Cloud Spanner does not have a DECIMAL/NUMERIC type, and we override the default mapping to
    // avoid Hibernate mapping these to NUMERIC(19,2) types.
    registerColumnType(Types.DECIMAL, "DECIMAL_TYPE_NOT_SUPPORTED");
    registerColumnType(Types.NUMERIC, "NUMERIC_TYPE_NOT_SUPPORTED");

    registerFunction("ANY_VALUE", new StandardSQLFunction("ANY_VALUE"));
    registerFunction("COUNTIF", new StandardSQLFunction("COUNTIF", StandardBasicTypes.LONG));

    registerFunction("CONCAT", new StandardSQLFunction("CONCAT"));
    registerFunction("STRING_AGG",
        new StandardSQLFunction("STRING_AGG", StandardBasicTypes.STRING));
    registerFunction("FARM_FINGERPRINT",
        new StandardSQLFunction("FARM_FINGERPRINT", StandardBasicTypes.LONG));
    registerFunction("SHA1", new StandardSQLFunction("SHA1", StandardBasicTypes.BINARY));
    registerFunction("SHA256", new StandardSQLFunction("SHA256", StandardBasicTypes.BINARY));
    registerFunction("SHA512", new StandardSQLFunction("SHA512", StandardBasicTypes.BINARY));
    registerFunction("BYTE_LENGTH",
        new StandardSQLFunction("BYTE_LENGTH", StandardBasicTypes.LONG));
    registerFunction("CHAR_LENGTH",
        new StandardSQLFunction("CHAR_LENGTH", StandardBasicTypes.LONG));
    registerFunction("CHARACTER_LENGTH",
        new StandardSQLFunction("CHARACTER_LENGTH", StandardBasicTypes.LONG));
    registerFunction("CODE_POINTS_TO_BYTES",
        new StandardSQLFunction("CODE_POINTS_TO_BYTES", StandardBasicTypes.BINARY));
    registerFunction("CODE_POINTS_TO_STRING",
        new StandardSQLFunction("CODE_POINTS_TO_STRING", StandardBasicTypes.STRING));
    registerFunction("ENDS_WITH", new StandardSQLFunction("ENDS_WITH", StandardBasicTypes.BOOLEAN));
    registerFunction("FORMAT", new StandardSQLFunction("FORMAT", StandardBasicTypes.STRING));
    registerFunction("FROM_BASE64",
        new StandardSQLFunction("FROM_BASE64", StandardBasicTypes.BINARY));
    registerFunction("FROM_HEX", new StandardSQLFunction("FROM_HEX", StandardBasicTypes.BINARY));
    registerFunction("LENGTH", new StandardSQLFunction("LENGTH", StandardBasicTypes.LONG));
    registerFunction("LPAD", new StandardSQLFunction("LPAD"));
    registerFunction("LOWER", new StandardSQLFunction("LOWER"));
    registerFunction("LTRIM", new StandardSQLFunction("LTRIM"));
    registerFunction("REGEXP_CONTAINS",
        new StandardSQLFunction("REGEXP_CONTAINS", StandardBasicTypes.BOOLEAN));
    registerFunction("REGEXP_EXTRACT", new StandardSQLFunction("REGEXP_EXTRACT"));
    registerFunction("REGEXP_EXTRACT_ALL",
        new StandardSQLFunction("REGEXP_EXTRACT_ALL", StringArrayType.INSTANCE));
    registerFunction("REGEXP_REPLACE", new StandardSQLFunction("REGEXP_REPLACE"));
    registerFunction("REPLACE", new StandardSQLFunction("REPLACE"));
    registerFunction("REPEAT", new StandardSQLFunction("REPEAT"));
    registerFunction("REVERSE", new StandardSQLFunction("REVERSE"));
    registerFunction("RPAD", new StandardSQLFunction("RPAD"));
    registerFunction("RTRIM", new StandardSQLFunction("RTRIM"));
    registerFunction("SAFE_CONVERT_BYTES_TO_STRING",
        new StandardSQLFunction("SAFE_CONVERT_BYTES_TO_STRING", StandardBasicTypes.STRING));
    registerFunction("SPLIT", new StandardSQLFunction("SPLIT", StringArrayType.INSTANCE));
    registerFunction("STARTS_WITH",
        new StandardSQLFunction("STARTS_WITH", StandardBasicTypes.BOOLEAN));
    registerFunction("STRPOS", new StandardSQLFunction("STRPOS", StandardBasicTypes.LONG));
    registerFunction("SUBSTR", new StandardSQLFunction("SUBSTR"));
    registerFunction("TO_BASE64", new StandardSQLFunction("TO_BASE64", StandardBasicTypes.STRING));
    registerFunction("TO_CODE_POINTS",
        new StandardSQLFunction("TO_CODE_POINTS", Int64ArrayType.INSTANCE));
    registerFunction("TO_HEX", new StandardSQLFunction("TO_HEX", StandardBasicTypes.STRING));
    registerFunction("TRIM", new StandardSQLFunction("TRIM"));
    registerFunction("UPPER", new StandardSQLFunction("UPPER"));
    registerFunction("JSON_QUERY",
        new StandardSQLFunction("JSON_QUERY", StandardBasicTypes.STRING));
    registerFunction("JSON_VALUE",
        new StandardSQLFunction("JSON_VALUE", StandardBasicTypes.STRING));

    registerFunction("ARRAY_CONCAT", new StandardSQLFunction("ARRAY_CONCAT"));
    registerFunction("ARRAY_LENGTH",
        new StandardSQLFunction("ARRAY_LENGTH", StandardBasicTypes.LONG));
    registerFunction("ARRAY_TO_STRING",
        new StandardSQLFunction("ARRAY_TO_STRING", StandardBasicTypes.STRING));
    registerFunction("GENERATE_ARRAY", new StandardSQLFunction("GENERATE_ARRAY") {
      @Override
      public Type getReturnType(Type firstArgumentType, Mapping mapping) {
        if (firstArgumentType == null) {
          return Int64ArrayType.INSTANCE;
        } else {
          if (firstArgumentType.equals(StandardBasicTypes.BYTE)
              || firstArgumentType.equals(StandardBasicTypes.SHORT)
              || firstArgumentType.equals(StandardBasicTypes.INTEGER)
              || firstArgumentType.equals(StandardBasicTypes.LONG)) {
            return Int64ArrayType.INSTANCE;
          } else if (firstArgumentType.equals(StandardBasicTypes.FLOAT)
              || firstArgumentType.equals(StandardBasicTypes.DOUBLE)) {
            return Float64ArrayType.INSTANCE;
          }
        }
        throw new IllegalArgumentException("Invalid input type for GENERATE_ARRAY: "
            + firstArgumentType.getName() + ". Only INT64 or FLOAT64 types are supported.");
      }
    });
    registerFunction("GENERATE_DATE_ARRAY",
        new StandardSQLFunction("GENERATE_DATE_ARRAY", DateArrayType.INSTANCE));
    registerFunction("ARRAY_REVERSE", new StandardSQLFunction("ARRAY_REVERSE"));

    registerFunction("CURRENT_DATE",
        new StandardSQLFunction("CURRENT_DATE", StandardBasicTypes.DATE));
    registerFunction("EXTRACT", new StandardSQLFunction("EXTRACT", StandardBasicTypes.LONG));
    registerFunction("DATE", new StandardSQLFunction("DATE", StandardBasicTypes.DATE));
    registerFunction("DATE_ADD", new StandardSQLFunction("DATE_ADD", StandardBasicTypes.DATE));
    registerFunction("DATE_SUB", new StandardSQLFunction("DATE_SUB", StandardBasicTypes.DATE));
    registerFunction("DATE_DIFF", new StandardSQLFunction("DATE_DIFF", StandardBasicTypes.LONG));
    registerFunction("DATE_TRUNC", new StandardSQLFunction("DATE_TRUNC", StandardBasicTypes.DATE));
    registerFunction("DATE_FROM_UNIX_DATE",
        new StandardSQLFunction("DATE_FROM_UNIX_DATE", StandardBasicTypes.DATE));
    registerFunction("FORMAT_DATE",
        new StandardSQLFunction("FORMAT_DATE", StandardBasicTypes.STRING));
    registerFunction("PARSE_DATE", new StandardSQLFunction("PARSE_DATE", StandardBasicTypes.DATE));
    registerFunction("UNIX_DATE", new StandardSQLFunction("UNIX_DATE", StandardBasicTypes.LONG));

    registerFunction("CURRENT_TIMESTAMP",
        new StandardSQLFunction("CURRENT_TIMESTAMP", StandardBasicTypes.TIMESTAMP));
    registerFunction("STRING", new StandardSQLFunction("STRING", StandardBasicTypes.STRING));
    registerFunction("TIMESTAMP",
        new StandardSQLFunction("TIMESTAMP", StandardBasicTypes.TIMESTAMP));
    registerFunction("TIMESTAMP_ADD",
        new StandardSQLFunction("TIMESTAMP_ADD", StandardBasicTypes.TIMESTAMP));
    registerFunction("TIMESTAMP_SUB",
        new StandardSQLFunction("TIMESTAMP_SUB", StandardBasicTypes.TIMESTAMP));
    registerFunction("TIMESTAMP_DIFF",
        new StandardSQLFunction("TIMESTAMP_DIFF", StandardBasicTypes.LONG));
    registerFunction("TIMESTAMP_TRUNC",
        new StandardSQLFunction("TIMESTAMP_TRUNC", StandardBasicTypes.TIMESTAMP));
    registerFunction("FORMAT_TIMESTAMP",
        new StandardSQLFunction("FORMAT_TIMESTAMP", StandardBasicTypes.STRING));
    registerFunction("PARSE_TIMESTAMP",
        new StandardSQLFunction("PARSE_TIMESTAMP", StandardBasicTypes.TIMESTAMP));
    registerFunction("TIMESTAMP_SECONDS",
        new StandardSQLFunction("TIMESTAMP_SECONDS", StandardBasicTypes.TIMESTAMP));
    registerFunction("TIMESTAMP_MILLIS",
        new StandardSQLFunction("TIMESTAMP_MILLIS", StandardBasicTypes.TIMESTAMP));
    registerFunction("TIMESTAMP_MICROS",
        new StandardSQLFunction("TIMESTAMP_MICROS", StandardBasicTypes.TIMESTAMP));
    registerFunction("UNIX_SECONDS",
        new StandardSQLFunction("UNIX_SECONDS", StandardBasicTypes.LONG));
    registerFunction("UNIX_MILLIS",
        new StandardSQLFunction("UNIX_MILLIS", StandardBasicTypes.LONG));
    registerFunction("UNIX_MICROS",
        new StandardSQLFunction("UNIX_MICROS", StandardBasicTypes.LONG));
    registerFunction("PARSE_TIMESTAMP",
        new StandardSQLFunction("PARSE_TIMESTAMP", StandardBasicTypes.TIMESTAMP));

    registerFunction("BIT_AND", new StandardSQLFunction("BIT_AND", StandardBasicTypes.LONG));
    registerFunction("BIT_OR", new StandardSQLFunction("BIT_OR", StandardBasicTypes.LONG));
    registerFunction("BIT_XOR", new StandardSQLFunction("BIT_XOR", StandardBasicTypes.LONG));
    registerFunction("LOGICAL_AND",
        new StandardSQLFunction("LOGICAL_AND", StandardBasicTypes.BOOLEAN));
    registerFunction("LOGICAL_OR",
        new StandardSQLFunction("LOGICAL_OR", StandardBasicTypes.BOOLEAN));

    registerFunction("IS_INF", new StandardSQLFunction("IS_INF", StandardBasicTypes.BOOLEAN));
    registerFunction("IS_NAN", new StandardSQLFunction("IS_NAN", StandardBasicTypes.BOOLEAN));

    registerFunction("SIGN", new StandardSQLFunction("SIGN"));
    registerFunction("IEEE_DIVIDE",
        new StandardSQLFunction("IEEE_DIVIDE", StandardBasicTypes.DOUBLE));
    registerFunction("SQRT", new StandardSQLFunction("SQRT", StandardBasicTypes.DOUBLE));
    registerFunction("POW", new StandardSQLFunction("POW", StandardBasicTypes.DOUBLE));
    registerFunction("POWER", new StandardSQLFunction("POWER", StandardBasicTypes.DOUBLE));
    registerFunction("EXP", new StandardSQLFunction("EXP", StandardBasicTypes.DOUBLE));
    registerFunction("LN", new StandardSQLFunction("LN", StandardBasicTypes.DOUBLE));
    registerFunction("LOG", new StandardSQLFunction("LOG", StandardBasicTypes.DOUBLE));
    registerFunction("LOG10", new StandardSQLFunction("LOG10", StandardBasicTypes.DOUBLE));
    registerFunction("GREATEST", new StandardSQLFunction("GREATEST"));
    registerFunction("LEAST", new StandardSQLFunction("LEAST"));
    registerFunction("DIV", new StandardSQLFunction("DIV", StandardBasicTypes.LONG));
    registerFunction("MOD", new StandardSQLFunction("MOD", StandardBasicTypes.LONG));
    registerFunction("ROUND", new StandardSQLFunction("ROUND", StandardBasicTypes.DOUBLE));
    registerFunction("TRUNC", new StandardSQLFunction("TRUNC", StandardBasicTypes.DOUBLE));
    registerFunction("CEIL", new StandardSQLFunction("CEIL", StandardBasicTypes.DOUBLE));
    registerFunction("CEILING", new StandardSQLFunction("CEILING", StandardBasicTypes.DOUBLE));
    registerFunction("FLOOR", new StandardSQLFunction("FLOOR", StandardBasicTypes.DOUBLE));
    registerFunction("COS", new StandardSQLFunction("COS", StandardBasicTypes.DOUBLE));
    registerFunction("COSH", new StandardSQLFunction("COSH", StandardBasicTypes.DOUBLE));
    registerFunction("ACOS", new StandardSQLFunction("ACOS", StandardBasicTypes.DOUBLE));
    registerFunction("ACOSH", new StandardSQLFunction("ACOSH", StandardBasicTypes.DOUBLE));
    registerFunction("SIN", new StandardSQLFunction("SIN", StandardBasicTypes.DOUBLE));
    registerFunction("SINH", new StandardSQLFunction("SINH", StandardBasicTypes.DOUBLE));
    registerFunction("ASIN", new StandardSQLFunction("ASIN", StandardBasicTypes.DOUBLE));
    registerFunction("ASINH", new StandardSQLFunction("ASINH", StandardBasicTypes.DOUBLE));
    registerFunction("TAN", new StandardSQLFunction("TAN", StandardBasicTypes.DOUBLE));
    registerFunction("TANH", new StandardSQLFunction("TANH", StandardBasicTypes.DOUBLE));
    registerFunction("ATAN", new StandardSQLFunction("ATAN", StandardBasicTypes.DOUBLE));
    registerFunction("ATANH", new StandardSQLFunction("ATANH", StandardBasicTypes.DOUBLE));
    registerFunction("ATAN2", new StandardSQLFunction("ATAN2", StandardBasicTypes.DOUBLE));

    tableExporter = createTableExporter(this);
    uniqueDelegate = createUniqueDelegate(this);
  }

  @Override
  public char openQuote() {
    return '`';
  }

  @Override
  public char closeQuote() {
    return '`';
  }

  @Override
  public IdentifierHelper buildIdentifierHelper(IdentifierHelperBuilder builder,
      DatabaseMetaData dbMetaData) throws SQLException {
    // we only override this method in order to be able to get our hands on the database metadata
    this.metadata = dbMetaData;
    builder.setAutoQuoteKeywords(true);
    return super.buildIdentifierHelper(builder, dbMetaData);
  }

  @Override
  public boolean dropConstraints() {
    return true;
  }

  @Override
  public String getCastTypeName(int code) {
    return getTypeNameWithoutLengthPrecisionAndScale(code);
  }

  private String getTypeNameWithoutLengthPrecisionAndScale(int code) {
    return getTypeName(code).replaceAll("\\(.+\\)", "");
  }

  abstract AbstractCloudSpannerUniqueDelegate createUniqueDelegate(
      AbstractCloudSpannerDialect dialect);

  @Override
  public UniqueDelegate getUniqueDelegate() {
    return uniqueDelegate;
  }

  @Override
  public boolean qualifyIndexName() {
    return false;
  }

  @Override
  public String getAddColumnString() {
    return "add column";
  }

  @Override
  public String toBooleanValueString(boolean bool) {
    return bool ? "true" : "false";
  }

  private static final class EmptyForeignKeyExporter implements Exporter<ForeignKey> {
    @Override
    public String[] getSqlCreateStrings(ForeignKey exportable, Metadata metadata) {
      return NO_COMMANDS;
    }

    @Override
    public String[] getSqlDropStrings(ForeignKey exportable, Metadata metadata) {
      return NO_COMMANDS;
    }
  }

  private EmptyForeignKeyExporter foreignKeyExporter = new EmptyForeignKeyExporter();

  @Override
  public Exporter<ForeignKey> getForeignKeyExporter() {
    return foreignKeyExporter;
  }

  abstract AbstractCloudSpannerTableExporter createTableExporter(
      AbstractCloudSpannerDialect dialect);

  @Override
  public Exporter<Table> getTableExporter() {
    return tableExporter;
  }

  @Override
  public boolean canCreateSchema() {
    return false;
  }

  @Override
  public LimitHandler getLimitHandler() {
    return LIMIT_HANDLER;
  }

  @Override
  public String getLimitString(String sql, boolean hasOffset) {
    return sql + (hasOffset ? " limit ? offset ?" : " limit ?");
  }

  @Override
  public boolean bindLimitParametersInReverseOrder() {
    return true;
  }

  @Override
  public boolean supportsUnionAll() {
    return true;
  }

  DatabaseMetaData getMetadata() {
    return metadata;
  }

}
