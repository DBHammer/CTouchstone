package ecnu.db.schema;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import ecnu.db.utils.exception.TouchstoneException;

import java.util.*;

/**
 * @author wangqingshuai
 */
public class Table {
    private int tableSize;
    private List<String> primaryKeys;
    private List<String> canonicalColumnNames;
    private Map<String, String> foreignKeys = new HashMap<>();
    @JsonIgnore
    private long joinTag = 1;

    public Table() {
    }

    public Table(List<String> canonicalColumnNames, int tableSize, List<String> primaryKeys) {
        this.canonicalColumnNames = canonicalColumnNames;
        this.tableSize = tableSize;
        this.primaryKeys = primaryKeys;
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

    public synchronized long getJoinTag() {
        long temp = joinTag;
        joinTag *= 4;
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


    public int getTableSize() {
        return tableSize;
    }

    public void setTableSize(int tableSize) {
        this.tableSize = tableSize;
    }

    public void setPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
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

    public synchronized void setPrimaryKeys(String primaryKeys) throws TouchstoneException {
        if (this.primaryKeys == null) {
            this.primaryKeys = Arrays.asList(primaryKeys.split(","));
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

    @Override
    public String toString() {
        return "Schema{tableSize=" + tableSize + '}';
    }
}
