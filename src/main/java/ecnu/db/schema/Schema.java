package ecnu.db.schema;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import ecnu.db.constraintchain.chain.ConstraintChain;
import ecnu.db.constraintchain.chain.ConstraintChainFkJoinNode;
import ecnu.db.exception.CannotFindColumnException;
import ecnu.db.exception.CannotFindSchemaException;
import ecnu.db.exception.TouchstoneToolChainException;
import ecnu.db.generation.JoinInfoTable;
import ecnu.db.schema.column.*;

import java.io.*;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static ecnu.db.Generator.SINGLE_THREAD_TUPLE_SIZE;
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

    /**
     * 准备好生成tuple
     * @param size 需要生成的tuple的大小
     * @param chains 约束链条
     * @param schemas 所有的表的map
     * @param directory joinInfoTable文件存放路径
     * @param neededThreads 每个joinInfoTable至少需要的thread数量
     * @throws TouchstoneToolChainException 生成失败
     */
    public void prepareTuples(int size, Collection<ConstraintChain> chains, Map<String, Schema> schemas, String directory, int neededThreads) throws TouchstoneToolChainException, IOException, InterruptedException {
        if (primaryKeys != null) { // primary keys is null, means no table will depend on this tables' join info table, thus it is ignored
            joinInfoTable = new JoinInfoTable(primaryKeys.split(",").length);
        }
        Map<Integer, boolean[]> pkBitMap = new HashMap<>();
        Table<String, ConstraintChainFkJoinNode, boolean[]> fkBitMap = HashBasedTable.create();
        for (ConstraintChain chain : chains) {
            chain.evaluate(this, size, pkBitMap, fkBitMap);
        }
        if (primaryKeys != null) {
            initJoinInfoTable(size, pkBitMap);
        }
        for (Map.Entry<String, Map<ConstraintChainFkJoinNode, boolean[]>> entry : fkBitMap.rowMap().entrySet()) {
            Map<ConstraintChainFkJoinNode, boolean[]> fkBitMap4Join = entry.getValue();
            List<ConstraintChainFkJoinNode> nodes = new ArrayList<>(fkBitMap4Join.keySet()).stream()
                    .sorted(Comparator.comparingInt(ConstraintChainFkJoinNode::getPkTag)).collect(Collectors.toList());
            for (int i = 0; i < size; i++) {
                long bitMap = 1L;
                for (ConstraintChainFkJoinNode node : nodes) {
                    bitMap = ((fkBitMap4Join.get(node)[i] ? 1L : 0L) & (bitMap << 1));
                }
                AbstractColumn refColumn =  schemas.get(nodes.get(i).getRefTable()).getColumn(nodes.get(i).getRefCol()), localColumn = getColumn(nodes.get(i).getFkCol());
                int maximumThreads = (schemas.get(nodes.get(i).getRefTable()).getTableSize() + SINGLE_THREAD_TUPLE_SIZE - 1) / SINGLE_THREAD_TUPLE_SIZE;
                if (maximumThreads < neededThreads) {
                    neededThreads = maximumThreads == 1 ? 1 : maximumThreads / 2;
                }
                int cnt;
                do {
                    File file = new File(directory, nodes.get(i).getRefTable());
                    if (file.isDirectory()) {
                        cnt = Objects.requireNonNull(file.listFiles()).length;
                    } else {
                        cnt = 0;
                    }
                    if (cnt < neededThreads) {
                        TimeUnit.MICROSECONDS.sleep(100); // if not ok, wait a little while before we check again.
                    }
                } while (cnt < neededThreads);
                File[] files = Objects.requireNonNull(new File(directory, nodes.get(i).getRefTable()).listFiles());
                JoinInfoTable joinInfoTable = new JoinInfoTable();
                for (File file : files) {
                    JoinInfoTable joinInfoTableTmp = new JoinInfoTable();
                    joinInfoTableTmp.readExternal(new ObjectInputStream(new FileInputStream(file)));
                    joinInfoTable.mergeJoinInfo(joinInfoTableTmp);
                }
                int pk = joinInfoTable.getPrimaryKey(bitMap)[0];
                localColumn.setTupleByRefColumn(refColumn, i, pk);
            }
        }
        if (primaryKeys != null) {
            File tmpFile = new File(String.format("%s_%s_%d.tmp", directory, tableName, Thread.currentThread().getId()));
            joinInfoTable.writeExternal(new ObjectOutputStream(new FileOutputStream(tmpFile)));
            Files.move(tmpFile.toPath(), new File(new File(directory, tableName), Long.toString(Thread.currentThread().getId())).toPath(), ATOMIC_MOVE);
        }
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

    public String generateInsertSqls(int size, String newDatabaseName) {
        Map<String, List<String>> columnName2Data = new HashMap<>();
        StringBuilder sqls = new StringBuilder();
        for (String columnName : columns.keySet()) {
            AbstractColumn column = columns.get(columnName);
            List<String> data;
            switch (column.getColumnType()) {
                case DATE:
                    data = Arrays.stream(((DateColumn) column).getTupleData()).map((d) -> String.format("'%s'", DateColumn.FMT.format(d))).collect(Collectors.toList());
                    break;
                case DATETIME:
                    data = Arrays.stream(((DateTimeColumn) column).getTupleData()).map((d) -> String.format("'%s'", DateTimeColumn.FMT.format(d))).collect(Collectors.toList());
                    break;
                case INTEGER:
                    data = Arrays.stream(((IntColumn) column).getTupleData()).mapToObj(Integer::toString).collect(Collectors.toList());
                    break;
                case DECIMAL:
                    data = Arrays.stream(((DecimalColumn) column).getTupleData()).mapToObj((d) -> BigDecimal.valueOf(d).toString()).collect(Collectors.toList());
                    break;
                case VARCHAR:
                    data = Arrays.stream(((StringColumn) column).getTupleData()).map((d) -> String.format("'%s'", d)).collect(Collectors.toList());
                    break;
                case BOOL:
                default:
                    throw new UnsupportedOperationException();
            }
            columnName2Data.put(columnName, data);
        }
        String tableName = this.tableName;
        if (newDatabaseName != null) {
            tableName = String.format("%s.%s", newDatabaseName, tableName.split("\\.")[1]);
        }
        for (int i = 0; i < size; i++) {
            String sql = String.format("insert into %s ", tableName);
            List<String> colNames = new ArrayList<>(), colVals = new ArrayList<>();
            for (String columnName : columnName2Data.keySet()) {
                colNames.add(columnName);
                colVals.add(columnName2Data.get(columnName).get(i));
            }
            sql += String.format("(%s) value (%s);\n", String.join(",", colNames), String.join(",", colVals));
            sqls.append(sql);
        }
        return sqls.toString();
    }
}
