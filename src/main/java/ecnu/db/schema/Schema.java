package ecnu.db.schema;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import ecnu.db.schema.column.*;
import ecnu.db.utils.ColumnConvert;
import ecnu.db.utils.exception.TouchstoneException;

import java.util.*;

/**
 * @author wangqingshuai
 */
public class Schema {
    private int tableSize;
    private List<String> primaryKeys;
    private List<String> canonicalColumnNames;
    private Map<String, String> foreignKeys = new HashMap<>();
    private int joinTag;

    public Schema() {
    }

    public Schema(String canonicalTableName, List<String> columnsMetadata) throws TouchstoneException {
        List<String> canonicalColumnNames = new ArrayList<>();
        for (String columnMetadata : columnsMetadata) {
            String[] attributes = columnMetadata.trim().split(" ");
            String canonicalColumnName = canonicalTableName + '.' + attributes[0];
            canonicalColumnNames.add(canonicalColumnName);
            int indexOfBrackets = attributes[1].indexOf('(');
            String dataType = (indexOfBrackets > 0) ? attributes[1].substring(0, indexOfBrackets) : attributes[1];
            switch (ColumnConvert.getColumnType(dataType)) {
                case INTEGER:
                    ColumnManager.getInstance().addColumn(canonicalColumnName, new IntColumn());
                    break;
                case BOOL:
                    ColumnManager.getInstance().addColumn(canonicalColumnName, new BoolColumn());
                    break;
                case DECIMAL:
                    ColumnManager.getInstance().addColumn(canonicalColumnName, new DecimalColumn());
                    break;
                case VARCHAR:
                    ColumnManager.getInstance().addColumn(canonicalColumnName, new StringColumn());
                    break;
                case DATE:
                case DATETIME:
                    DateTimeColumn column = new DateTimeColumn();
                    if (indexOfBrackets > 0) {
                        column.setPrecision(Integer.parseInt(attributes[1].substring(indexOfBrackets + 1, attributes[1].length() - 1)));
                    } else {
                        column.setPrecision(0);
                    }
                    ColumnManager.getInstance().addColumn(canonicalColumnName, column);
                    break;
                default:
                    throw new TouchstoneException("没有实现的类型转换");
            }
        }
        this.canonicalColumnNames = canonicalColumnNames;
        joinTag = 1;
    }

    public List<String> getCanonicalColumnNames() {
        return canonicalColumnNames;
    }

    @JsonIgnore
    public List<String> getCanonicalColumnNamesNotFk() {
        List<String> canonicalColumnNamesNotKey = new LinkedList<>(canonicalColumnNames);
        canonicalColumnNamesNotKey.removeAll(primaryKeys);
        canonicalColumnNamesNotKey.removeAll(foreignKeys.keySet());
        return canonicalColumnNamesNotKey;
    }

    public int getJoinTag() {
        int temp = joinTag;
        joinTag += 1;
        return temp;
    }

    public void setJoinTag(int joinTag) {
        this.joinTag = joinTag;
    }


    /**
     * 本表的列是否参照目标列
     *
     * @param localColumn 本表列
     * @param refColumn   目标列
     * @return 参照时返回true，否则返回false
     */
    public boolean isRefTable(String localColumn, String refColumn) {
        if (foreignKeys.containsKey(localColumn)) {
            return refColumn.equals(foreignKeys.get(localColumn));
        } else {
            return false;
        }
    }

    public void addForeignKey(String localTable, String localColumnName,
                              String referencingTable, String referencingInfo) throws TouchstoneException {
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
            foreignKeys.put(localTable + "." + columnNames[i], referencingTable + "." + refColumnNames[i]);
            primaryKeys.remove(columnNames[i]);
        }
    }


    public int getTableSize() {
        return tableSize;
    }

    public void setTableSize(int tableSize) {
        this.tableSize = tableSize;
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
        return primaryKeys.toString();
    }

    public void setPrimaryKeys(String primaryKeys) throws TouchstoneException {
        if (this.primaryKeys == null) {
            this.primaryKeys = new LinkedList<>(Arrays.asList(primaryKeys.split(",")));
        } else {
            Set<String> newKeys = new HashSet<>(Arrays.asList(primaryKeys.split(",")));
            Set<String> keys = new HashSet<>(this.primaryKeys);
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
        return "Schema{tableSize=" + tableSize + '}';
    }
}
