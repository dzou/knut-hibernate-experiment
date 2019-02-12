package knut.dialect;

import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.Arrays;
import org.hibernate.type.AbstractSingleColumnStandardBasicType;
import org.hibernate.type.descriptor.ValueBinder;
import org.hibernate.type.descriptor.ValueExtractor;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.AbstractTypeDescriptor;
import org.hibernate.type.descriptor.java.JavaTypeDescriptor;
import org.hibernate.type.descriptor.sql.BasicExtractor;
import org.hibernate.type.descriptor.sql.SqlTypeDescriptor;

/** Type definition for ARRAY<INT64> columns in Cloud Spanner. */
public class Int64ArrayType extends AbstractSingleColumnStandardBasicType<Long[]> {
  private static final long serialVersionUID = 1L;
  public static final Int64ArrayType INSTANCE = new Int64ArrayType();

  static final class Int64ArrayTypeDescriptor extends AbstractTypeDescriptor<Long[]> {
    private static final long serialVersionUID = 1L;
    private static final Int64ArrayTypeDescriptor INSTANCE = new Int64ArrayTypeDescriptor();

    private Int64ArrayTypeDescriptor() {
      super(Long[].class);
    }

    @Override
    public Long[] fromString(String string) {
      throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <X> X unwrap(Long[] value, Class<X> type, WrapperOptions options) {
      if (value != null) {
        return (X) Arrays.copyOf(value, value.length, Object[].class);
      }
      return null;
    }

    @Override
    public <X> Long[] wrap(X value, WrapperOptions options) {
      if (value instanceof Array) {
        Array array = (Array) value;
        try {
          Object data = array.getArray();
          if (data instanceof Long[]) {
            return (Long[]) data;
          }
          if (data instanceof Object[]) {
            Object[] objectArray = (Object[]) data;
            return Arrays.copyOf(objectArray, objectArray.length, Long[].class);
          }
          throw new IllegalArgumentException(
              data.getClass().getName() + " is not a valid type for a INT64 array");
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
      return (Long[]) value;
    }

    @Override
    public String toString(Long[] value) {
      return value == null ? "null" : value.toString();
    }
  }

  static class Int64ArraySqlTypeDescriptor implements SqlTypeDescriptor {
    private static final long serialVersionUID = 1L;
    private static final Int64ArraySqlTypeDescriptor INSTANCE = new Int64ArraySqlTypeDescriptor();

    private Int64ArraySqlTypeDescriptor() {}

    @Override
    public int getSqlType() {
      return Types.ARRAY;
    }

    @Override
    public boolean canBeRemapped() {
      return false;
    }

    @Override
    public <X> ValueBinder<X> getBinder(final JavaTypeDescriptor<X> javaTypeDescriptor) {
      return new ValueBinder<X>() {
        @Override
        public void bind(PreparedStatement st, X value, int index, WrapperOptions options)
            throws SQLException {
          st.setArray(index, st.getConnection().createArrayOf("INT64",
              javaTypeDescriptor.unwrap(value, Long[].class, options)));
        }

        @Override
        public void bind(CallableStatement st, X value, String name, WrapperOptions options)
            throws SQLException {
          throw new SQLFeatureNotSupportedException("Callable statements are not supported.");
        }
      };
    }

    @Override
    public <X> ValueExtractor<X> getExtractor(final JavaTypeDescriptor<X> javaTypeDescriptor) {
      return new BasicExtractor<X>(javaTypeDescriptor, this) {
        @Override
        protected X doExtract(ResultSet rs, String name, WrapperOptions options)
            throws SQLException {
          return javaTypeDescriptor.wrap(rs.getArray(name), options);
        }

        @Override
        protected X doExtract(CallableStatement statement, int index, WrapperOptions options)
            throws SQLException {
          throw new SQLFeatureNotSupportedException("Callable statements are not supported.");
        }

        @Override
        protected X doExtract(CallableStatement statement, String name, WrapperOptions options)
            throws SQLException {
          throw new SQLFeatureNotSupportedException("Callable statements are not supported.");
        }
      };
    }
  }

  public Int64ArrayType() {
    super(Int64ArraySqlTypeDescriptor.INSTANCE, Int64ArrayTypeDescriptor.INSTANCE);
  }

  @Override
  public String getName() {
    return "INT64_ARRAY";
  }
}
