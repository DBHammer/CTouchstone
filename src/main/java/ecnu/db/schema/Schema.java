package ecnu.db.schema;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;
import ecnu.db.exception.TouchstoneException;
import ecnu.db.exception.schema.CannotFindSchemaException;
import ecnu.db.utils.CommonUtils;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * @author wangqingshuai
 */
public class Schema {
    private String canonicalTableName;
    private int tableSize;
    private String primaryKeys;
    private List<String> canonicalColumnNames;
    private Map<String, String> foreignKeys;
    /**
     * 根据Database的metadata获取的外键信息
     */
    private Map<String, String> metaDataFks;
    private int joinTag;

    public Schema() {
    }

    public List<String> getCanonicalColumnNames() {
        return canonicalColumnNames;
    }

    public Schema(String canonicalTableName, List<String> canonicalColumnNames) {
        this.canonicalTableName = canonicalTableName;
        this.canonicalColumnNames = canonicalColumnNames;
        joinTag = 1;
    }

    /**
     * 初始化Schema.foreignKeys和Schema.metaDataFks
     *
     * @param metaData 数据库的元信息
     * @param schemas  需要初始化的表
     * @throws SQLException        无法从数据库的metadata中获取信息
     * @throws TouchstoneException 没有找到主键/外键表，或者外键关系冲突
     */
    public static void initFks(DatabaseMetaData metaData, Map<String, Schema> schemas) throws SQLException, TouchstoneException {
        for (Map.Entry<String, Schema> entry : schemas.entrySet()) {
            String[] canonicalTableName = entry.getKey().split("\\.");
            ResultSet rs = metaData.getImportedKeys(null, canonicalTableName[0], canonicalTableName[1]);
            while (rs.next()) {
                String pkTable = rs.getString("PKTABLE_NAME"), pkCol = rs.getString("PKCOLUMN_NAME"),
                        fkTable = rs.getString("FKTABLE_NAME"), fkCol = rs.getString("FKCOLUMN_NAME");
                if (!schemas.containsKey(fkTable)) {
                    throw new CannotFindSchemaException(fkTable);
                }
                schemas.get(fkTable).addForeignKey(fkCol, pkTable, pkCol);
            }
        }

        for (Map.Entry<String, Schema> entry : schemas.entrySet()) {
            Schema schema = entry.getValue();
            Map<String, String> fks = Optional.ofNullable(schema.getForeignKeys()).orElse(new HashMap<>(CommonUtils.INIT_HASHMAP_SIZE));
            schema.setMetaDataFks(fks);
        }
    }


    public int getJoinTag() {
        int temp = joinTag;
        joinTag += 1;
        return temp;
    }

    public void setJoinTag(int joinTag) {
        this.joinTag = joinTag;
    }

    public void addForeignKey(String localColumnName, String referencingTable, String referencingInfo) throws TouchstoneException {
        String[] columnNames = localColumnName.split(",");
        String[] refColumnNames = referencingInfo.split(",");
        if (foreignKeys == null) {
            foreignKeys = new HashMap<>(columnNames.length);
        }
        for (int i = 0; i < columnNames.length; i++) {
            if (foreignKeys.containsKey(columnNames[i])) {
                if (!(referencingTable + "." + refColumnNames[i]).equals(foreignKeys.get(columnNames[i]))) {
                    throw new TouchstoneException("冲突的主外键连接");
                } else {
                    return;
                }
            }
            foreignKeys.put(columnNames[i], referencingTable + "." + refColumnNames[i]);
        }

    }

    public String getCanonicalTableName() {
        return canonicalTableName;
    }

    @JsonSetter
    @SuppressWarnings("unused")
    public void setCanonicalTableName(String canonicalTableName) {
        this.canonicalTableName = canonicalTableName;
    }

    public int getTableSize() {
        return tableSize;
    }

    public void setTableSize(int tableSize) {
        this.tableSize = tableSize;
    }

    public Map<String, String> getMetaDataFks() {
        return metaDataFks;
    }

    public void setMetaDataFks(Map<String, String> metaDataFks) {
        this.metaDataFks = metaDataFks;
    }

    public Map<String, String> getForeignKeys() {
        return foreignKeys;
    }

    @JsonSetter
    @SuppressWarnings("unused")
    public void setForeignKeys(Map<String, String> foreignKeys) {
        this.foreignKeys = foreignKeys;
    }

    @JsonGetter
    @SuppressWarnings("unused")
    public String getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(String primaryKeys) throws TouchstoneException {
        if (this.primaryKeys == null) {
            this.primaryKeys = primaryKeys;
        } else {
            Set<String> newKeys = new HashSet<>(Arrays.asList(primaryKeys.split(",")));
            Set<String> keys = new HashSet<>(Arrays.asList(this.primaryKeys.split(",")));
            if (keys.size() == newKeys.size()) {
                keys.removeAll(newKeys);
                if (keys.size() > 0) {
                    throw new TouchstoneException("query中使用了多列主键的部分主键");
                }
            } else {
                throw new TouchstoneException("query中使用了多列主键的部分主键");
            }
        }
    }

    @Override
    public String toString() {
        return "Schema{" +
                "tableName='" + canonicalTableName + '\'' +
                ", tableSize=" + tableSize +
                '}';
    }
}
