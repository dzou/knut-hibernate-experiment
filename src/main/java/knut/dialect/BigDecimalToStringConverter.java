package knut.dialect;

import java.math.BigDecimal;
import java.util.Arrays;
import javax.persistence.AttributeConverter;
import org.hibernate.HibernateException;

/** Default converter of {@link BigDecimal} attributes to STRING(38) columns. */
public class BigDecimalToStringConverter implements AttributeConverter<BigDecimal, String> {
  static final BigDecimal MAX_BIG_DECIMAL_VALUE =
      new BigDecimal("999999999999999999.999999999999999999");
  static final BigDecimal MIN_BIG_DECIMAL_VALUE =
      new BigDecimal("-999999999999999999.999999999999999999");
  static final int MAX_BIG_DECIMAL_SCALE = 18;
  static final int MAX_BIG_DECIMAL_INT_LENGTH = 18;
  static final int MAX_BIG_DECIMAL_STRING_LENGTH = 38; // 18 + 18 + 1 (decimal point) + 1 (leading
                                                       // minus sign)
  /**
   * All {@link BigDecimal} values will be written in the database with leading zeros to guarantee
   * correct ordering
   */
  private static final char[] ZEROS = new char[MAX_BIG_DECIMAL_INT_LENGTH];
  static {
    Arrays.fill(ZEROS, '0');
  }

  @Override
  public String convertToDatabaseColumn(BigDecimal attribute) {
    if (attribute == null) {
      return null;
    }
    checkValidBigDecimal(attribute);
    String string = attribute.toPlainString();
    StringBuilder res = new StringBuilder(MAX_BIG_DECIMAL_STRING_LENGTH - string.length());
    if (attribute.compareTo(BigDecimal.ZERO) < 0) {
      res.append("-")
          .append(ZEROS, 0,
              MAX_BIG_DECIMAL_INT_LENGTH
                  - (string.length() - attribute.scale() - (attribute.scale() == 0 ? 1 : 2)))
          .append(string.substring(1));
    } else {
      res.append(ZEROS, 0,
          MAX_BIG_DECIMAL_INT_LENGTH
              - (string.length() - attribute.scale() - (attribute.scale() == 0 ? 0 : 1)))
          .append(string);
    }
    return res.toString();
  }

  @Override
  public BigDecimal convertToEntityAttribute(String dbData) {
    if (dbData == null) {
      return null;
    }
    BigDecimal decimal = new BigDecimal(dbData);
    checkValidBigDecimal(decimal);
    return decimal;
  }

  private void checkValidBigDecimal(BigDecimal value) throws HibernateException {
    if (value.scale() < 0) {
      throw new HibernateException("BigDecimal values with negative scale are not supported");
    }
    if (value.scale() > MAX_BIG_DECIMAL_SCALE) {
      throw new HibernateException("BigDecimal values with scale greater than "
          + MAX_BIG_DECIMAL_SCALE + " are not supported");
    }
    if (value.compareTo(MAX_BIG_DECIMAL_VALUE) > 0) {
      throw new HibernateException("BigDecimal values greater than "
          + convertToDatabaseColumn(MAX_BIG_DECIMAL_VALUE) + " are not supported");
    }
    if (value.compareTo(MIN_BIG_DECIMAL_VALUE) < 0) {
      throw new HibernateException("BigDecimal values less than "
          + convertToDatabaseColumn(MIN_BIG_DECIMAL_VALUE) + " are not supported");
    }
  }

}
