package ecnu.db;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import ecnu.db.generator.constraintchain.ConstraintChain;
import ecnu.db.generator.constraintchain.ConstraintChainNode;
import ecnu.db.generator.constraintchain.filter.*;
import ecnu.db.generator.constraintchain.filter.operation.CompareOperator;
import ecnu.db.generator.constraintchain.filter.operation.UniVarFilterOperation;
import ecnu.db.generator.constraintchain.join.ConstraintChainFkJoinNode;
import ecnu.db.generator.constraintchain.join.ConstraintChainPkJoinNode;
import ecnu.db.schema.Column;
import ecnu.db.schema.ColumnType;
import ecnu.db.schema.Table;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.exception.TouchstoneException;

import java.io.*;
import java.math.BigDecimal;
import java.util.*;

import static ecnu.db.generator.constraintchain.filter.operation.CompareOperator.EQ;
import static ecnu.db.utils.CommonUtils.readFile;

public class getInputTouchstone {
    private static String schemaInfoPath = "D:\\eclipse-workspace\\Mirage\\resultBIBENCH\\schema.json";

    private static String columnInfoPath = "D:\\eclipse-workspace\\Mirage\\resultBIBENCH\\column.csv";

    private static String constraintChainPath = "D:\\eclipse-workspace\\Mirage\\resultBIBENCH\\workload";

    private static final LinkedHashMap<String, Column> columns = new LinkedHashMap<>();

    private static LinkedHashMap<String, Table> schemas = new LinkedHashMap<>();

    private static Map<String, List<ConstraintChain>> constraintChains = new TreeMap<>();

    public static final CsvMapper CSV_MAPPER = new CsvMapper();

    private static final CsvSchema columnSchema = CSV_MAPPER.schemaFor(Column.class);

    private static final String[] changeableTableInTPCDS = {"public.store", "public.store_sales", "public.item", "public.promotion",
            "public.inventory", "public.catalog_sales", "public.store_returns", "public.catalog_returns", "public.customer_address",
            "public.customer", "public.web_sales", "public.call_center", "public.web_page", "public.web_returns"};

    private static final int SF = 1;

    public static void main(String[] args) throws IOException, TouchstoneException {
        getColumnInput();
        String columnInfo = getColumnInfoString();
        //System.out.println(columnInfo);
        String schemaInfo = getSchemaInput();
        //System.out.println(schemaInfo);
        String outputPath1 = "D:\\eclipse-workspace\\Touchstone\\conf\\bibench_schema_sf_1.txt";
        FileWriter fw = new FileWriter(outputPath1);
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(schemaInfo);
        bw.write("\n");
        bw.write(columnInfo);
        bw.close();
        fw.close();
        getConstraintChainInput();
        String allCCInfo = getConstraintString();
        String outputPath2 = "D:\\eclipse-workspace\\Touchstone\\conf\\bibench_cardinality_constraints_sf_1.txt";
        FileWriter fw2 = new FileWriter(outputPath2);
        BufferedWriter bw2 = new BufferedWriter(fw2);
        bw2.write(allCCInfo);
        bw2.close();
        fw2.close();
    }

    private static void getColumnInput() throws IOException {
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(columnInfoPath))) {
            bufferedReader.readLine();
            String line;
            LinkedHashMap<String, Column> tmpColumns = new LinkedHashMap<>();
            while ((line = bufferedReader.readLine()) != null) {
                int commaIndex = line.indexOf(",");
                String columnData = line.substring(commaIndex + 1);
                Column column = CSV_MAPPER.readerFor(Column.class).with(columnSchema).readValue(columnData);
                column.init();
                tmpColumns.put(line.substring(0, commaIndex), column);
            }
            List<String> columnSequence = getColumnSequence();
            for (String s : columnSequence) {
                columns.put(s, tmpColumns.get(s));
            }
        }
    }

    private static List<String> getColumnSequence() throws IOException {
        LinkedHashMap<String, Table> schemas = CommonUtils.MAPPER.readValue(CommonUtils.readFile("D:\\eclipse-workspace\\Mirage\\resultBIBENCH\\schema.json"), new TypeReference<>() {
        });
        List<String> columnSequence = new ArrayList<>();
        for (Map.Entry<String, Table> stringTableEntry : schemas.entrySet()) {
            List<String> allColumns = stringTableEntry.getValue().getCanonicalColumnNames();
            List<String> tempPrimayKeys = stringTableEntry.getValue().getPrimaryKeysList();
            Map<String, String> foreignKeys = stringTableEntry.getValue().getForeignKeys();
            tempPrimayKeys.removeAll(foreignKeys.keySet());
            allColumns.removeAll(tempPrimayKeys);
            allColumns.removeAll(foreignKeys.keySet());
            List<String> foreignKeysList = new ArrayList<>(foreignKeys.keySet());
            Collections.sort(foreignKeysList);
            allColumns.addAll(0, foreignKeysList);
            allColumns.addAll(0, tempPrimayKeys);
            columnSequence.addAll(allColumns);
        }
        return columnSequence;
    }

    private static String getColumnInfoString() throws TouchstoneException {
        StringBuilder allColumnInfo = new StringBuilder();
        for (Map.Entry<String, Column> column : columns.entrySet()) {
            StringBuilder columnInfo = new StringBuilder("D[");
            String columnName = column.getKey();
            columnName = columnName.replaceFirst("public\\.", "");
            Column currentColumn = column.getValue();
            columnInfo.append(getEachColumnInfo(columnName, currentColumn));
            columnInfo.append("]\n");
            allColumnInfo.append(columnInfo);
        }
        return allColumnInfo.toString();
    }

    private static String getEachColumnInfo(String columnName, Column currentColumn) throws TouchstoneException {
        if (columnName.equals("date_dim.d_date") ||
                columnName.equals("customer_address.ca_gmt_offset") ||
                columnName.equals("store.s_gmt_offset")) {
            currentColumn.setColumnType(ColumnType.INTEGER);
            currentColumn.setSpecialValue(1);
        }
        if (columnName.equals("store.s_gmt_offset")) {
            currentColumn.setRange(100);
        }
        String columnInfo = switch (currentColumn.getColumnType()) {
            case INTEGER ->
                    columnName + "; " + currentColumn.getNullPercentage() + "; " + currentColumn.getRange() + "; "
                            + currentColumn.transferDataToValue(0) + "; "
                            + currentColumn.transferDataToValue(currentColumn.getRange());
            case DECIMAL, DATETIME, DATE -> columnName + "; " + currentColumn.getNullPercentage() + "; "
                    + currentColumn.transferDataToValue(0) + "; "
                    + currentColumn.transferDataToValue(currentColumn.getRange());
            case VARCHAR -> columnName + "; " + currentColumn.getNullPercentage() + "; "
                    + (currentColumn.getAvgLength() + currentColumn.getMaxLength()) / 2 + "; "
                    + currentColumn.getMaxLength();
            case BOOL -> throw new TouchstoneException("no bool in Mirage");
        };
        return columnInfo;
    }

    private static String getSchemaInput() throws IOException {
        StringBuilder schemeInfo = new StringBuilder();
        schemas = CommonUtils.MAPPER.readValue(CommonUtils.readFile(schemaInfoPath), new TypeReference<>() {
        });
        HashSet<String> enhanceSchema = new HashSet<>();
        for (Map.Entry<String, Table> schema : schemas.entrySet()) {
            StringBuilder currentSchemaInfo = new StringBuilder("T[");
            String tableName = schema.getKey();
            currentSchemaInfo.append(tableName.split("\\.")[1]).append("; ");
            Table currentTable = schema.getValue();
            long tableSize = currentTable.getTableSize();
            if (tableSize < 500) {
                tableSize *= 100;
            }
//            if(Arrays.stream(changeableTableInTPCDS).toList().contains(tableName)){
//                tableSize = tableSize*SF;
//            }
            currentSchemaInfo.append(tableSize).append("; ");
            //加列信息
            List<String> columnNameList = getEachColumnSeq(currentTable);
            int start = 0;
            for (String cname : columnNameList) {
                String columnType = columns.get(cname).getColumnType().toString();
                String simpleColomnName = cname.split("\\.")[2];
                if (start != columnNameList.size() - 1) {
                    currentSchemaInfo.append(simpleColomnName).append(", ").append(columnType).append("; ");
                } else {
                    currentSchemaInfo.append(simpleColomnName).append(", ").append(columnType);
                }
                start++;
            }
            //加主键信息
            List<String> primaryKeys = currentTable.getPrimaryKeysList();
            if (primaryKeys.size() != 0) {
                currentSchemaInfo.append(" P(");
                for (int i = 0; i < primaryKeys.size(); i++) {
                    String simplePK = primaryKeys.get(i).split("\\.")[2];
                    if (i == (primaryKeys.size() - 1)) {
                        currentSchemaInfo.append(simplePK);
                    } else {
                        currentSchemaInfo.append(simplePK).append(", ");
                    }
                }
                if (currentTable.getForeignKeys().size() == 0) {
                    currentSchemaInfo.append(")");
                } else {
                    currentSchemaInfo.append("); ");
                }
            }
            //加外键信息
            Map<String, String> foreignKeys = currentTable.getForeignKeys();
            if (foreignKeys.size() != 0) {
                int i = 1;
                for (Map.Entry<String, String> foreignKey : foreignKeys.entrySet()) {
                    if (i != foreignKeys.size()) {
                        String fkInfo = "F(" + foreignKey.getKey().split("\\.")[2] + ", "
                                + foreignKey.getValue().split("\\.")[1] + "." + foreignKey.getValue().split("\\.")[2]
                                + "); ";
                        currentSchemaInfo.append(fkInfo);
                    } else {
                        String fkInfo = "F(" + foreignKey.getKey().split("\\.")[2] + ", "
                                + foreignKey.getValue().split("\\.")[1] + "." + foreignKey.getValue().split("\\.")[2]
                                + ")";
                        currentSchemaInfo.append(fkInfo);
                    }
                    i++;
                }
            }
            currentSchemaInfo.append("]\n");
            schemeInfo.append(currentSchemaInfo);
        }
        return schemeInfo.toString();
    }

    private static List<String> getEachColumnSeq(Table currentTable) {
        List<String> allColumns = currentTable.getCanonicalColumnNames();
        List<String> tempPrimayKeys = currentTable.getPrimaryKeysList();
        Map<String, String> foreignKeys = currentTable.getForeignKeys();
        tempPrimayKeys.removeAll(foreignKeys.keySet());
        allColumns.removeAll(tempPrimayKeys);
        allColumns.removeAll(foreignKeys.keySet());
        List<String> foreignKeysList = new ArrayList<>(foreignKeys.keySet());
        Collections.sort(foreignKeysList);
        allColumns.addAll(0, foreignKeysList);
        allColumns.addAll(0, tempPrimayKeys);
        return allColumns;
    }

    private static void getConstraintChainInput() throws IOException {
        File sqlDic = new File(constraintChainPath);
        File[] sqlArray = sqlDic.listFiles();
        assert sqlArray != null;
        for (File file : sqlArray) {
            File[] graphArray = file.listFiles();
            assert graphArray != null;
            for (File file1 : graphArray) {
                if (file1.getName().contains("json")) {
                    Map<String, List<ConstraintChain>> eachresult = CommonUtils.MAPPER.readValue(readFile(file1.getPath()), new TypeReference<>() {
                    });
                    constraintChains.putAll(eachresult);
                }
            }
        }
    }

    private static String getConstraintString() throws TouchstoneException {
        StringBuilder allCCInfo = new StringBuilder();
        for (Map.Entry<String, List<ConstraintChain>> cc : constraintChains.entrySet()) {
            List<ConstraintChain> ccInEachSQL = cc.getValue();
            String queryName = cc.getKey();
            allCCInfo.append("## ").append(queryName).append("\n");
            for (ConstraintChain constraintChain : ccInEachSQL) {
                StringBuilder currentCC = new StringBuilder();
                String tableName = constraintChain.getTableName();
                currentCC.append("[").append(tableName.split("\\.")[1]).append("]; ");
                List<ConstraintChainNode> nodes = constraintChain.getNodes();
                int i = 1;
                for (ConstraintChainNode node : nodes) {
                    switch (node.getConstraintChainNodeType()) {
                        case FILTER -> handleFilterNode(currentCC, (ConstraintChainFilterNode) node);
                        case PK_JOIN -> handlePKNode(currentCC, (ConstraintChainPkJoinNode) node);
                        case FK_JOIN -> handleFKNode(currentCC, (ConstraintChainFkJoinNode) node);
                    }
                    if (i != nodes.size()) {
                        currentCC.append("; ");
                    }
                    i++;
                }
                //System.out.println(currentCC);
                allCCInfo.append(currentCC).append("\n");
            }
        }
        return allCCInfo.toString();
    }

    private static void handleFilterNode(StringBuilder cc, ConstraintChainFilterNode node) throws TouchstoneException {
        StringBuilder filterInfo = new StringBuilder("[0, ");
        BigDecimal probability = node.getProbability();
        LogicNode root = node.getRoot();
        BoolExprNode realRoot = root.getChildren().get(0);
        BoolExprType type = realRoot.getType();
        if (type == BoolExprType.AND || type == BoolExprType.OR) {
            if (realRoot instanceof LogicNode) {
                StringBuilder boolInfo = new StringBuilder();
                List<BoolExprNode> children = ((LogicNode) realRoot).getChildren();
                int i = 1;
                HashMap<String, List<CompareOperator>> column2Operators = new HashMap<>();
                for (BoolExprNode child : children) {
                    if (child.getType() == BoolExprType.UNI_FILTER_OPERATION) {
                        UniVarFilterOperation uniVarFilterOperation = (UniVarFilterOperation) child;
                        column2Operators.putIfAbsent(uniVarFilterOperation.getCanonicalColumnName(), new ArrayList<>());
                        column2Operators.get(uniVarFilterOperation.getCanonicalColumnName()).add(uniVarFilterOperation.getOperator());
                        String uniVarInfo = handleUnivar(uniVarFilterOperation);
                        if (i == children.size()) {
                            boolInfo.append(uniVarInfo);
                        } else {
                            boolInfo.append(uniVarInfo).append("#");
                        }
                        i++;
                    } else {
                        //throw new TouchstoneException("cantdeal");
                    }
                }
                HashSet<String> betColumns = new HashSet<>();
                for (var value : column2Operators.entrySet()) {
                    if (value.getValue().size() == 2) {
                        betColumns.add(value.getKey().split("\\.")[2]);
                    }
                }
                String[] infos = boolInfo.toString().split("#");
                HashSet<String> removeColumns = new HashSet<>();
                StringBuilder finalInfo = new StringBuilder();
                for (String info : infos) {
                    String column = info.split("@")[0];
                    if (betColumns.remove(column)) {
                        removeColumns.add(column);
                        finalInfo.append(column).append("@bet#");
                    } else if (!removeColumns.contains(column)) {
                        finalInfo.append(info).append("#");
                    }
                }
                if (!boolInfo.isEmpty()) {
                    filterInfo.append(finalInfo).append(type).append(", ");
                }
            } else {
                //throw new TouchstoneException("cantdeal");
            }
        } else if (type == BoolExprType.UNI_FILTER_OPERATION) {
            UniVarFilterOperation operation = (UniVarFilterOperation) realRoot;
            String uniVarInfo = handleUnivar(operation);
            filterInfo.append(uniVarInfo).append(", ");
        } else {
            throw new TouchstoneException("cantdeal");
        }
        if (filterInfo.length() != 0) {
            filterInfo.append(probability).append("]");
        }
        cc.append(filterInfo);
    }

    private static String handleUnivar(UniVarFilterOperation operation) {
        StringBuilder uniVarInfo = new StringBuilder();
        String columnName = operation.getCanonicalColumnName().split("\\.")[2];
        uniVarInfo.append(columnName).append("@");

        CompareOperator op = operation.getOperator();
        if (op == EQ) {
            ColumnType columnType = columns.get(operation.getCanonicalColumnName()).getColumnType();
            if (columnType != ColumnType.INTEGER && columnType != ColumnType.VARCHAR) {
                System.out.println(operation);
                System.out.println("wqs");
            }
        }
        switch (op) {
            case EQ -> uniVarInfo.append("=");
            case LT -> uniVarInfo.append("<");
            case LE -> uniVarInfo.append("<=");
            case GT -> uniVarInfo.append(">");
            case GE -> uniVarInfo.append(">=");
            case LIKE -> uniVarInfo.append("like");
            case IN -> uniVarInfo.append("in").append("(").append(operation.getParameters().size()).append(")");
            case NE -> uniVarInfo.append("<>");
            case NOT_IN -> uniVarInfo.append("not in");
            case NOT_LIKE -> uniVarInfo.append("not like");
        }
//        System.out.println(operation.getParameters().stream().mapToInt(Parameter::getId).boxed().toList());
        return uniVarInfo.toString();
    }

    private static void handlePKNode(StringBuilder cc, ConstraintChainPkJoinNode node) {
        StringBuilder pkInfo = new StringBuilder("[1, ");
        String columnName = node.getPkColumns()[0];
        int joinTag = node.getPkTag();
        int firstTag = (int) Math.pow(2, (2 * joinTag));
        int secondTag = (int) Math.pow(2, (2 * joinTag + 1));
        pkInfo.append(columnName).append(", ").append(firstTag).append(", ").append(secondTag).append("]");
        cc.append(pkInfo);
    }

    private static void handleFKNode(StringBuilder cc, ConstraintChainFkJoinNode node) {
        StringBuilder fkInfo = new StringBuilder("[2, ");
        String localCol = node.getLocalCols().split("\\.")[2];
        String refCol = node.getRefCols().split("\\.")[1] + "." + node.getRefCols().split("\\.")[2];
        BigDecimal pb = node.getProbability();
        int joinTag = node.getPkTag();
        int firstTag = (int) Math.pow(2, (2 * joinTag));
        int secondTag = (int) Math.pow(2, (2 * joinTag + 1));
        fkInfo.append(localCol).append(", ").append(pb).append(", ").append(refCol).append(", ").append(firstTag).append(", ").append(secondTag).append("]");
        cc.append(fkInfo);
    }
}
