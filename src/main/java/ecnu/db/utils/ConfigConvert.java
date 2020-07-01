package ecnu.db.utils;

import ecnu.db.dbconnector.DumpFileConnector;
import ecnu.db.schema.column.ColumnType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class ConfigConvert {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigConvert.class);

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
