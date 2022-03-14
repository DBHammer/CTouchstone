package ecnu.db.schema;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import ecnu.db.dbconnector.DbConnector;
import ecnu.db.utils.exception.TouchstoneException;

import java.sql.SQLException;
import java.util.*;

/**
 * @author wangqingshuai
 */
public class Table {
    private long tableSize;
    private List<String> primaryKeys;
    private List<String> canonicalColumnNames;
    private Map<String, String> foreignKeys = new HashMap<>();
    @JsonIgnore
    private int joinTag = 0;

    public Table() {
        this.primaryKeys = new ArrayList<>();
    }

    public Table(List<String> canonicalColumnNames, long tableSize) {
        this.canonicalColumnNames = canonicalColumnNames;
        this.tableSize = tableSize;
        this.primaryKeys = new ArrayList<>();
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

    public synchronized int getJoinTag() {
        return joinTag++;
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

    public boolean isRefTable(String refTable) {
        return foreignKeys.values().stream().anyMatch(remoteColumn -> remoteColumn.contains(refTable));
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


    public long getTableSize() {
        return tableSize;
    }

    public void setTableSize(long tableSize) {
        this.tableSize = tableSize;
    }

    public Map<String, String> getForeignKeys() {
        return foreignKeys;
    }

    @JsonSetter
    @SuppressWarnings("unused")
    public synchronized void setForeignKeys(Map<String, String> foreignKeys) {
        this.foreignKeys = foreignKeys;
    }

    @JsonGetter
    @SuppressWarnings("unused")
    public String getPrimaryKeys() {
        return String.join(",", primaryKeys);
    }

    public void setPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    public synchronized void setPrimaryKeys(String primaryKeys) throws TouchstoneException {
        if (this.primaryKeys.isEmpty()) {
            this.primaryKeys.addAll(List.of(primaryKeys.split(",")));
        } else {
            Set<String> newKeys = new HashSet<>(Arrays.asList(primaryKeys.split(",")));
            Set<String> keys = new HashSet<>(this.primaryKeys);
            if (keys.size() == newKeys.size()) {
                keys.removeAll(newKeys);
                if (!keys.isEmpty()) {
                    throw new TouchstoneException("query中使用了多列主键的部分主键");
                }
            } else {
                throw new TouchstoneException("query中使用了多列主键的部分主键");
            }
        }
    }

    @JsonIgnore
    public List<String> getPrimaryKeysList() {
        return primaryKeys;
    }

    public void toSQL(DbConnector dbConnector, String tableName) throws SQLException, TouchstoneException {
        StringBuilder head = new StringBuilder("CREATE TABLE ");
        head.append(tableName).append(" (");
        List<String> allColumns = new ArrayList<>(canonicalColumnNames);
        List<String> tempPrimayKeys = new ArrayList<>(primaryKeys);
        tempPrimayKeys.removeAll(foreignKeys.keySet());
        allColumns.removeAll(tempPrimayKeys);
        allColumns.removeAll(foreignKeys.keySet());
        List<String> foreignKeysList = new ArrayList<>(foreignKeys.keySet());
        Collections.sort(foreignKeysList);
        allColumns.addAll(0, foreignKeysList);
        allColumns.addAll(0, tempPrimayKeys);
        for (String canonicalColumnName : allColumns) {
            String columnName = canonicalColumnName.split("\\.")[2].toUpperCase();
            Column column = ColumnManager.getInstance().getColumn(canonicalColumnName);
            String addColumn = columnName + " " + column.getOriginalType() + ",";
            head.append(addColumn);
        }
        head = new StringBuilder(head.substring(0, head.length() - 1));
        head.append(");");
        dbConnector.executeSql(head.toString());
    }

    @Override
    public String toString() {
        return "Schema{tableSize=" + tableSize + '}';
    }
}
