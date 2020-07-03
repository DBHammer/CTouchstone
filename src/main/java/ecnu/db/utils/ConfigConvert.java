package ecnu.db.utils;

import ecnu.db.schema.column.ColumnType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * @author qingshuai.wang
 */
public class ConfigConvert {

    private static HashMap<HashSet<String>, ColumnType> typeConvert;

    public static void setTypeConvert(HashMap<ColumnType, HashSet<String>> typeConvert) {
        ConfigConvert.typeConvert = new HashMap<>(typeConvert.size());
        for (Map.Entry<ColumnType, HashSet<String>> columnTypeHashSetEntry : typeConvert.entrySet()) {
            ConfigConvert.typeConvert.put(columnTypeHashSetEntry.getValue(), columnTypeHashSetEntry.getKey());
        }
    }

    public static ColumnType getColumnType(String readType) throws TouchstoneToolChainException {
        for (Map.Entry<HashSet<String>, ColumnType> hashSetColumnTypeEntry : typeConvert.entrySet()) {
            if (hashSetColumnTypeEntry.getKey().contains(readType)) {
                return hashSetColumnTypeEntry.getValue();
            }
        }
        throw new TouchstoneToolChainException("数据类型" + readType + "的匹配模版没有指定");
    }
}
