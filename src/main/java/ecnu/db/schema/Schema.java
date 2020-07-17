package ecnu.db.schema;

import ecnu.db.schema.column.AbstractColumn;
import ecnu.db.schema.column.ColumnType;
import ecnu.db.utils.TouchstoneToolChainException;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.*;

/**
 * @author wangqingshuai
 */
public class Schema {
    private final static int INIT_HASHMAP_SIZE = 16;
    private String tableName;
    private HashMap<String, AbstractColumn> columns;
    private int tableSize;
    private String primaryKeys;
    private HashMap<String, String> foreignKeys;
    /**
     * 根据Database的metadata获取的外键信息
     */
    private HashMap<String, String> metaDataFks;
    private int joinTag;
    private int lastJoinTag;

    public Schema() {
    }

    public Schema(String tableName, HashMap<String, AbstractColumn> columns) {
        this.tableName = tableName;
        this.columns = columns;
        joinTag = 1;
        lastJoinTag = 1;
    }

    /**
     * 判定给定的表格是否满足全局拓扑序
     *
     * @param schemas 待验证的表格
     * @return 是否存在全局拓扑序
     */
    public static boolean existsTopologicalOrderOrNot(Collection<Schema> schemas) {
        HashSet<String> topologicalTables = new HashSet<>();
        for (int i = 0; i < schemas.size(); i++) {
            for (Schema schema : schemas) {
                if (!topologicalTables.contains(schema.getTableName()) && schema.onlyReferencingTables(topologicalTables)) {
                    topologicalTables.add(schema.getTableName());
                    break;
                }
            }
            if (i == topologicalTables.size()) {
                return false;
            }
        }
        return true;
    }

    /**
     * 初始化Schema.foreignKeys和Schema.metaDataFks
     *
     * @param metaData 数据库的元信息
     * @param schemas  需要初始化的表
     * @throws SQLException
     * @throws TouchstoneToolChainException
     */
    public static void initFks(DatabaseMetaData metaData, HashMap<String, Schema> schemas) throws SQLException, TouchstoneToolChainException {
        for (Map.Entry<String, Schema> entry : schemas.entrySet()) {
            String tableName = entry.getKey();
            ResultSet rs = metaData.getImportedKeys(null, null, tableName);
            while (rs.next()) {
                String pkTable = rs.getString("PKTABLE_NAME"), pkCol = rs.getString("PKCOLUMN_NAME"),
                        fkTable = rs.getString("FKTABLE_NAME"), fkCol = rs.getString("FKCOLUMN_NAME");
                schemas.get(fkTable).addForeignKey(fkCol, pkTable, pkCol);
            }
        }

        for (Map.Entry<String, Schema> entry : schemas.entrySet()) {
            Schema schema = entry.getValue();
            HashMap<String, String> fks = Optional.ofNullable(schema.getForeignKeys()).orElse(new HashMap<>(INIT_HASHMAP_SIZE));
            schema.setMetaDataFks(fks);
        }
    }

    /**
     * 判断本表是否只依赖于这些表，用于确定是否存在全局拓扑序
     *
     * @param tableNames 已经确定存在拓扑序的表格
     * @return 是否只依赖于这些表
     */
    public boolean onlyReferencingTables(HashSet<String> tableNames) {
        if (foreignKeys != null) {
            for (String referencingTableInfo : foreignKeys.values()) {
                if (!tableNames.contains(referencingTableInfo.split("\\.")[0])) {
                    return false;
                }
            }
        }
        return true;
    }

    public int getJoinTag() {
        int temp = joinTag;
        joinTag *= 4;
        return temp;
    }

    public void setJoinTag(int joinTag) {
        this.joinTag = joinTag;
    }

    public void keepJoinTag(boolean keep) {
        if (keep) {
            lastJoinTag = joinTag;
        } else {
            joinTag = lastJoinTag;
        }
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

    public int getNdv(String columnName) throws TouchstoneToolChainException {
        if (!columns.containsKey(columnName)) {
            throw new TouchstoneToolChainException("不存在的列" + columnName);
        }
        return columns.get(columnName).getNdv();
    }


    /**
     * 格式化返回schema信息
     *
     * @return 返回满足touchstone格式的schema信息
     */
    public String formatSchemaInfo() {
        if (primaryKeys == null && foreignKeys == null) {
            return null;
        }
        int k = 1;
        //todo 最小表大小重置应该废弃
        if (tableSize < 100) {
            k = 100;
        }
        StringBuilder schemaInfo = new StringBuilder("T[" + tableName + ';' + tableSize * k + ';');
        List<String> hasProduct = new ArrayList<>();
        for (AbstractColumn column : columns.values()) {
            if (primaryKeys != null) {
                if (primaryKeys.contains(column.getColumnName())) {
                    schemaInfo.append(column.formatColumnType());
                    hasProduct.add(column.getColumnName());
                }
            }
        }

        for (AbstractColumn column : columns.values()) {
            if (!hasProduct.contains(column.getColumnName())) {
                if (foreignKeys != null) {
                    if (foreignKeys.containsKey(column.getColumnName())) {
                        schemaInfo.append(column.formatColumnType());
                        hasProduct.add(column.getColumnName());
                    }
                }
            }
        }

        for (AbstractColumn column : columns.values()) {
            if (!hasProduct.contains(column.getColumnName())) {
                schemaInfo.append(column.formatColumnType());
            }
        }

        if (primaryKeys != null) {
            schemaInfo.append("P(").append(primaryKeys).append(");");
        } else {
            schemaInfo.append("P(");
            for (String localKey : foreignKeys.keySet()) {
                schemaInfo.append(localKey).append(",");
            }
            schemaInfo.replace(schemaInfo.length(), schemaInfo.length(), ");");
        }

        if (foreignKeys != null) {
            for (Map.Entry<String, String> keyAndRefKey : foreignKeys.entrySet()) {
                schemaInfo.append("F(").append(keyAndRefKey.getKey()).append(',').
                        append(keyAndRefKey.getValue()).append(");");
            }
        }
        return schemaInfo.replace(schemaInfo.length() - 1, schemaInfo.length(), "]").toString();
    }

    /**
     * 格式化返回数据分布信息
     *
     * @return 返回满足touchstone格式的数据分布
     */
    public String formatDataDistributionInfo() throws ParseException {
        if (primaryKeys == null && foreignKeys == null) {
            return null;
        }
        StringBuilder dataDistributionInfo = new StringBuilder();
        for (AbstractColumn column : columns.values()) {
            boolean skip = false;
            if (primaryKeys != null && primaryKeys.contains(column.getColumnName())) {
                skip = true;
            }
            if (foreignKeys != null && foreignKeys.containsKey(column.getColumnName())) {
                skip = true;
            }
            if (!skip) {
                dataDistributionInfo.append("D[").append(tableName).append(".").
                        append(column.formatDataDistribution()).append("]").append(System.lineSeparator());
            }
        }
        return dataDistributionInfo.toString();
    }

    public boolean isDate(String columnName) {
        return columns.get(columnName).getColumnType() == ColumnType.DATETIME;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getTableSize() {
        return tableSize;
    }

    public void setTableSize(int tableSize) {
        this.tableSize = tableSize;
    }

    public HashMap<String, String> getMetaDataFks() {
        return metaDataFks;
    }

    public void setMetaDataFks(HashMap<String, String> metaDataFks) {
        this.metaDataFks = metaDataFks;
    }

    public HashMap<String, String> getForeignKeys() {
        return foreignKeys;
    }

    public void setForeignKeys(HashMap<String, String> foreignKeys) {
        this.foreignKeys = foreignKeys;
    }

    public String getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(String primaryKeys) throws TouchstoneToolChainException {
        if (this.primaryKeys == null) {
            this.primaryKeys = primaryKeys;
        } else {
            HashSet<String> newKeys = new HashSet<>(Arrays.asList(primaryKeys.split(",")));
            HashSet<String> keys = new HashSet<>(Arrays.asList(this.primaryKeys.split(",")));
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

    public int getLastJoinTag() {
        return lastJoinTag;
    }

    public void setLastJoinTag(int lastJoinTag) {
        this.lastJoinTag = lastJoinTag;
    }

    public HashMap<String, AbstractColumn> getColumns() {
        return columns;
    }

    public void setColumns(HashMap<String, AbstractColumn> columns) {
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
