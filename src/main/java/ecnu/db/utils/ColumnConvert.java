package ecnu.db.utils;

import ecnu.db.exception.TouchstoneToolChainException;
import ecnu.db.schema.column.ColumnType;

import java.util.HashMap;
import java.util.Map;

import static ecnu.db.schema.column.ColumnType.*;

/**
 * @author qingshuai.wang
 */
public class ColumnConvert {

    private static final Map<String, ColumnType> dataType2ColumnType;

    static {
        dataType2ColumnType = new HashMap<>();
        dataType2ColumnType.put("int", INTEGER);
        dataType2ColumnType.put("date", DATE);
        dataType2ColumnType.put("datetime", DATETIME);
        dataType2ColumnType.put("decimal", DECIMAL);
        dataType2ColumnType.put("varchar", VARCHAR);
        dataType2ColumnType.put("char", VARCHAR);
        dataType2ColumnType.put("bool", BOOL);
    }

    public static ColumnType getColumnType(String dataType) throws TouchstoneToolChainException {
        if (!dataType2ColumnType.containsKey(dataType)) {
            throw new TouchstoneToolChainException("数据类型" + dataType + "的匹配模版没有指定");
        }
        return dataType2ColumnType.get(dataType);
    }
}
