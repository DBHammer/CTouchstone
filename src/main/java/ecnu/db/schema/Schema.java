package ecnu.db.schema;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;
import ecnu.db.exception.CannotFindColumnException;
import ecnu.db.exception.CannotFindSchemaException;
import ecnu.db.exception.TouchstoneToolChainException;
import ecnu.db.schema.column.AbstractColumn;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * @author wangqingshuai
 */
public class Schema {
    private final static int INIT_HASHMAP_SIZE = 16;
    private String tableName;
    private Map<String, AbstractColumn> columns;
    private int tableSize;
    private String primaryKeys;
    private Map<String, String> foreignKeys;
    /**
     * 根据Database的metadata获取的外键信息
     */
    private Map<String, String> metaDataFks;
    private int joinTag;

    public Schema() {
    }

    public Schema(String tableName, Map<String, AbstractColumn> columns) {
        this.tableName = tableName;
        this.columns = columns;
        joinTag = 1;
    }

    /**
     * 初始化Schema.foreignKeys和Schema.metaDataFks
     *
     * @param metaData 数据库的元信息
     * @param schemas  需要初始化的表
     * @throws SQLException                 无法从数据库的metadata中获取信息
     * @throws TouchstoneToolChainException 没有找到主键/外键表，或者外键关系冲突
     */
    public static void initFks(DatabaseMetaData metaData, Map<String, Schema> schemas) throws SQLException, TouchstoneToolChainException {
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
            Map<String, String> fks = Optional.ofNullable(schema.getForeignKeys()).orElse(new HashMap<>(INIT_HASHMAP_SIZE));
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

    public void addForeignKey(String localColumnName, String referencingTable, String referencingInfo) throws TouchstoneToolChainException {
        String[] columnNames = localColumnName.split(",");
        String[] refColumnNames = referencingInfo.split(",");
        if (foreignKeys == null) {
            foreignKeys = new HashMap<>(columnNames.length);
        }
        for (int i = 0; i < columnNames.length; i++) {
            if (foreignKeys.containsKey(columnNames[i])) {
                if (!(referencingTable + "." + refColumnNames[i]).equals(foreignKeys.get(columnNames[i]))) {
                    throw new TouchstoneToolChainException("冲突的主外键连接");
                } else {
                    return;
                }
            }
            foreignKeys.put(columnNames[i], referencingTable + "." + refColumnNames[i]);
        }

    }

    public int getNdv(String columnName) throws CannotFindColumnException {
        if (!columns.containsKey(columnName)) {
            throw new CannotFindColumnException(tableName, columnName);
        }
        return columns.get(columnName).getNdv();
    }

    public String getTableName() {
        return tableName;
    }

    @JsonSetter
    @SuppressWarnings("unused")
    public void setTableName(String tableName) {
        this.tableName = tableName;
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

    public void setPrimaryKeys(String primaryKeys) throws TouchstoneToolChainException {
        if (this.primaryKeys == null) {
            this.primaryKeys = primaryKeys;
        } else {
            Set<String> newKeys = new HashSet<>(Arrays.asList(primaryKeys.split(",")));
            Set<String> keys = new HashSet<>(Arrays.asList(this.primaryKeys.split(",")));
            if (keys.size() == newKeys.size()) {
                keys.removeAll(newKeys);
                if (keys.size() > 0) {
                    throw new TouchstoneToolChainException("query中使用了多列主键的部分主键");
                }
            } else {
                throw new TouchstoneToolChainException("query中使用了多列主键的部分主键");
            }
        }
    }

    public AbstractColumn getColumn(String columnName) throws CannotFindColumnException {
        AbstractColumn column = columns.get(columnName);
        if (column == null) {
            throw new CannotFindColumnException(tableName, columnName);
        }
        return column;
    }

    public Map<String, AbstractColumn> getColumns() {
        return columns;
    }

    public void setColumns(Map<String, AbstractColumn> columns) {
        this.columns = columns;
    }

    @Override
    public String toString() {
        return "Schema{" +
                "tableName='" + tableName + '\'' +
                ", tableSize=" + tableSize +
                '}';
    }
}
