package ecnu.db.schema.column;

import com.fasterxml.jackson.annotation.JsonFormat;

/**
 * @author wangqingshuai
 */
@JsonFormat(shape = JsonFormat.Shape.STRING)
public enum ColumnType {
    /* 定义类型的列，可根据配置文件将类型映射到这些类型*/
    INTEGER, VARCHAR, DECIMAL, BOOL, DATETIME
}
