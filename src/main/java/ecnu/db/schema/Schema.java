package ecnu.db.schema;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import ecnu.db.constraintchain.chain.ConstraintChain;
import ecnu.db.constraintchain.chain.ConstraintChainFkJoinNode;
import ecnu.db.exception.TouchstoneException;
import ecnu.db.exception.schema.CannotFindColumnException;
import ecnu.db.exception.schema.CannotFindSchemaException;
import ecnu.db.generation.JoinInfoTable;
import ecnu.db.schema.column.*;

import java.io.*;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static ecnu.db.generation.Generator.SINGLE_THREAD_TUPLE_SIZE;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

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
    private JoinInfoTable joinInfoTable;
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

    public void init() {
        if (primaryKeys != null) {
            joinInfoTable = new JoinInfoTable(primaryKeys.length());
        }
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



    private void initJoinInfoTable(int size, Map<Integer, boolean[]> pkBitMap) {
        List<Integer> pks = new ArrayList<>(pkBitMap.keySet());
        pks.sort(Integer::compareTo);
        for (int i = 0; i < size; i++) {
            long bitMap = 1L;
            for (int pk : pks) {
                bitMap = ((pkBitMap.get(pk)[i] ? 1L : 0L) & (bitMap << 1));
            }
            joinInfoTable.addJoinInfo(bitMap, new int[]{i});
        }
    }

    public String transferData() {
        Map<String, List<String>> columnName2Data = new HashMap<>();
        for (String columnName : columns.keySet()) {
            AbstractColumn column = columns.get(columnName);
            List<String> data;
            switch (column.getColumnType()) {
                case DATE:
                    data = Arrays.stream(((DateColumn) column).getTupleData())
                            .parallel()
                            .map((d) -> String.format("'%s'", DateColumn.FMT.format(d)))
                            .collect(Collectors.toList());
                    break;
                case DATETIME:
                    data = Arrays.stream(((DateTimeColumn) column).getTupleData())
                            .parallel()
                            .map((d) -> String.format("'%s'", DateTimeColumn.FMT.format(d)))
                            .collect(Collectors.toList());
                    break;
                case INTEGER:
                    data = Arrays.stream(((IntColumn) column).getTupleData())
                            .parallel()
                            .mapToObj(Integer::toString)
                            .collect(Collectors.toList());
                    break;
                case DECIMAL:
                    data = Arrays.stream(((DecimalColumn) column).getTupleData())
                            .parallel()
                            .mapToObj((d) -> BigDecimal.valueOf(d).toString())
                            .collect(Collectors.toList());
                    break;
                case VARCHAR:
                    data = Arrays.stream(((StringColumn) column).getTupleData())
                            .parallel()
                            .map((d) -> String.format("'%s'", d))
                            .collect(Collectors.toList());
                    break;
                case BOOL:
                default:
                    throw new UnsupportedOperationException();
            }
            columnName2Data.put(columnName, data);
        }
        StringBuilder data = new StringBuilder();
        //todo 添加对于数据的格式化的处理
        return data.toString();
    }
}
