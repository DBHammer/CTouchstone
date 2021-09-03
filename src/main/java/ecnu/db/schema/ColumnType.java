package ecnu.db.schema;

import com.fasterxml.jackson.annotation.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;

/**
 * @author wangqingshuai
 */
@JsonFormat(shape = JsonFormat.Shape.STRING)
public enum ColumnType {
    /* 定义类型的列，可根据配置文件将类型映射到这些类型*/
    INTEGER, VARCHAR, DECIMAL, BOOL, DATE, DATETIME;

    private static final Logger logger = LoggerFactory.getLogger(ColumnType.class);

    public static ColumnType getColumnType(int dataType) {
        return switch (dataType) {
            case Types.INTEGER, Types.BIGINT -> INTEGER;
            case Types.VARCHAR, Types.CHAR -> VARCHAR;
            case Types.FLOAT, Types.DECIMAL, Types.NUMERIC -> DECIMAL;
            case Types.DATE -> DATE;
            case Types.TIMESTAMP -> DATETIME;
            default -> {
                logger.error("未支持的算子转换{}", dataType);
                throw new UnsupportedOperationException();
            }
        };
    }
}
