package ecnu.db.pg;

import com.google.common.base.Throwables;
import com.google.common.collect.Multimap;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import ecnu.db.analyzer.online.AbstractAnalyzer;
import ecnu.db.analyzer.online.ExecutionNode;
import ecnu.db.analyzer.online.RawNode;
import ecnu.db.constraintchain.filter.SelectResult;
import ecnu.db.dbconnector.DatabaseConnectorInterface;
import ecnu.db.exception.TouchstoneException;
import ecnu.db.exception.analyze.UnsupportedSelect;
import ecnu.db.pg.parser.PgSelectOperatorInfoLexer;
import ecnu.db.pg.parser.PgSelectOperatorInfoParser;
import ecnu.db.schema.Schema;
import ecnu.db.tidb.parser.TidbSelectOperatorInfoLexer;
import ecnu.db.tidb.parser.TidbSelectOperatorInfoParser;
import ecnu.db.utils.AbstractDatabaseInfo;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.config.PrepareConfig;
import java_cup.runtime.ComplexSymbolFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.w3c.dom.Node;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import ecnu.db.pg.PgNodeTypeInfo;

import static ecnu.db.utils.CommonUtils.matchPattern;

public class PgAnalyzer extends AbstractAnalyzer {
    private final PgSelectOperatorInfoParser parser = new PgSelectOperatorInfoParser(new PgSelectOperatorInfoLexer(new StringReader("")), new ComplexSymbolFactory());
    private static final Pattern JOIN_EQ_OPERATOR = Pattern.compile("Cond: \\(.*\\)");
    private static final Pattern EQ_OPERATOR = Pattern.compile("\\(([a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+) = ([a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+)\\)");
    public PgAnalyzer(PrepareConfig config, DatabaseConnectorInterface dbConnector, AbstractDatabaseInfo databaseInfo, Map<String, Schema> schemas, Multimap<String, String> tblName2CanonicalTblName) {
        super(config, dbConnector, databaseInfo, schemas, tblName2CanonicalTblName);
    }

    @Override
    protected String extractTableName(String tableName) {
        if (aliasDic.containsKey(tableName)) {
            tableName = aliasDic.get(tableName);
        }
        return tableName;
    }

    @Override
    public ExecutionNode getExecutionTree(List<String[]> queryPlan) throws TouchstoneException {
        StringBuilder myQueryPlan = new StringBuilder();
        for (String[] strings : queryPlan) {
            myQueryPlan.append(strings[0]);
        }
        String theQueryPlan=myQueryPlan.toString();
        return buildExecutionTree(theQueryPlan);
    }


    private ExecutionNode buildExecutionTree(String queryPlan) throws TouchstoneException {
        PgNodeTypeInfo NodeTypeRef = new PgNodeTypeInfo();
        ReadContext rc = JsonPath.parse(queryPlan);
        String Path = "$.[0]['Plan']";
        String NodeTypePath = Path + "['Node Type']";
        String NodeType = rc.read(NodeTypePath);
        while(NodeTypeRef.isPassNode(NodeType)){//找到第一个可以处理的节点
            Path+="['Plans'][0]";
            NodeTypePath = Path + "['Node Type']";
            NodeType = rc.read(NodeTypePath);
        }
        ExecutionNode node = getExecutionNode(queryPlan,Path);
        Stack<Pair<String,ExecutionNode>> stack = new Stack<>();
        Stack<Pair<String,ExecutionNode>> rootStack = new Stack<>();//存放根节点
        rootStack.push(Pair.of(Path,node));
        stack.push(Pair.of(Path,node));
        String currentPath;
        while(!stack.empty()){
            Pair<String,ExecutionNode> pair =stack.pop();
            Path = pair.getKey();
            node = pair.getValue();
            Map<String, String> allKeys = getKeys(queryPlan,Path);
            if(hasChildPlan(allKeys)){
                int plansCount = rc.read(Path+"['Plans'].length()");
                if(hasChildPlan(allKeys) && plansCount==2){
                    currentPath = Path+"['Plans'][1]";
                    String currentNodeType = rc.read(currentPath+"['Node Type']");
                    if(currentNodeType.equals("Hash")){
                        currentPath+="['Plans'][0]";
                    }
                    ExecutionNode currentNode = getExecutionNode(queryPlan,currentPath);
                    node.rightNode = currentNode;
                    stack.push(Pair.of(currentPath,currentNode));
                    plansCount--;
                }
                if(hasChildPlan(allKeys) && plansCount==1){
                    currentPath = Path+"['Plans'][0]";
                    ExecutionNode currentNode = getExecutionNode(queryPlan,currentPath);
                    node.leftNode = currentNode;
                    stack.push(Pair.of(currentPath,currentNode));
                }
            }
        }
        Pair<String,ExecutionNode> rootPair =rootStack.pop();
        ExecutionNode rootNode = rootPair.getValue();
        return rootNode;
    }

    private ExecutionNode getExecutionNode(String queryPlan,String Path) throws TouchstoneException{
        PgNodeTypeInfo NodeTypeRef = new PgNodeTypeInfo();
        ReadContext rc = JsonPath.parse(queryPlan);
        String NodeTypePath = Path + "['Node Type']";
        String NodeType = rc.read(NodeTypePath);
        String planId = Path;
        int rowCount = rc.read(Path+"['Actual Rows']");
        ExecutionNode node;
        if(NodeTypeRef.isFilterNode(NodeType)){
            Map<String, String> allKeys = getKeys(queryPlan,Path);
            if(hasFilterInfo(allKeys)){
                String filterInfo = rc.read(Path+"['Filter']");
                String tableName = rc.read(Path+"['Relation Name']");
                tableName = extractTableName(tableName);
                String canonicalTableName = getCanonicalTblName(tableName);
                String filterInfoTrans = transFilterInfo(filterInfo,canonicalTableName);
                node = new ExecutionNode(planId, ExecutionNode.ExecutionNodeType.filter,rowCount,filterInfoTrans);
                return node;
            }else{
                String tableName = rc.read(Path+"['Relation Name']");
                tableName = extractTableName(tableName);
                String canonicalTableName = getCanonicalTblName(tableName);
                node = new ExecutionNode(planId, ExecutionNode.ExecutionNodeType.scan,rowCount,"table:" + canonicalTableName);
                return node;
            }
        }else if(NodeTypeRef.isJoinNode(NodeType)){
            Map<String, String> allKeys = getKeys(queryPlan,Path);
            if(NodeType.equals("Hash Join")) {
                String joinInfo = rc.read(Path + "['Hash Cond']");
                joinInfo = "Hash Cond: "+joinInfo;
                if(hasJoinFilter(allKeys)){
                    String joinFilter = rc.read(Path + "['Join Filter']");
                    joinInfo = joinInfo + " " + "Join Filter: " + joinFilter;
                }
                node = new ExecutionNode(planId, ExecutionNode.ExecutionNodeType.join, rowCount, joinInfo);
                return node;
            }else if(NodeType.equals("Nested Loop")){
                String joinInfo = rc.read(Path+"['Plans'][1]['Index Cond']");
                String tableName =rc.read(Path+"['Plans'][1]['Relation Name']");
                String joinInfoTrans = transIndexCondInfo(joinInfo,tableName);
                joinInfo = "Index Cond: " + joinInfoTrans;
                node = new ExecutionNode(planId, ExecutionNode.ExecutionNodeType.join, rowCount, joinInfo);
                return node;
            }
        }
        return null;
    }
    private Map<String, String> getKeys(String queryPlan,String Path) throws TouchstoneException{
        ReadContext rc = JsonPath.parse(queryPlan);
        Map<String, String> allKeys= rc.read(Path);
        return allKeys;
    }

    private boolean hasChildPlan(Map<String, String> allKeys) throws TouchstoneException{
        for ( String key : allKeys.keySet() ) {
            if(key.equals("Plans")){
                return true;
            }
        }
        return false;
    }

    private boolean hasFilterInfo(Map<String, String> allKeys) throws TouchstoneException{
        for ( String key : allKeys.keySet() ) {
            if(key.equals("Filter")){
                return true;
            }
        }
        return false;
    }

    private boolean hasJoinFilter(Map<String, String> allKeys) throws TouchstoneException{
        for ( String key : allKeys.keySet() ) {
            if(key.equals("Join Filter")){
                return true;
            }
        }
        return false;
    }

    private String getCanonicalTblName(String tableName) throws TouchstoneException {
        if (CommonUtils.isCanonicalTableName(tableName)) {
            return tableName;
        }
        List<String> canonicalTblNames = new ArrayList<>(tblName2CanonicalTblName.get(tableName));
        if (canonicalTblNames.size() > 1) {
            throw new TouchstoneException(String.format("'%s'的表名有冲突，请使用别名",
                    canonicalTblNames
                            .stream()
                            .map((name) -> String.format("%s", name))
                            .collect(Collectors.joining(","))));
        }
        return canonicalTblNames.get(0);
    }

    public String transFilterInfo (String filterInfo, String tableName) throws TouchstoneException{
        Pattern COLNAME = Pattern.compile("[a-zA-Z]+\\_[a-zA-Z]+");
        Matcher m = COLNAME.matcher(filterInfo);
        StringBuffer info = new StringBuffer();
        while(m.find()){
            String colName = m.group();
            m.appendReplacement(info,tableName+"."+colName);
        }
        m.appendTail(info);
        return info.toString();
    }

    public String transIndexCondInfo (String indexCond, String tableName) throws TouchstoneException{
        Pattern COLNAME = Pattern.compile("[a-zA-Z]+\\_[a-zA-Z]+");
        Matcher m = COLNAME.matcher(indexCond);
        m.find();
        String colName = m.group(0);
        return m.replaceFirst(tableName+"."+colName);
    }

    @Override
    protected String[] analyzeJoinInfo(String joinInfo) throws TouchstoneException {
        if (joinInfo.contains("other cond:")) {
            throw new TouchstoneException("join中包含其他条件,暂不支持");
        }
        String[] result = new String[4];
        String leftTable, leftCol, rightTable, rightCol;
        Matcher eqCondition = JOIN_EQ_OPERATOR.matcher(joinInfo);
        if (eqCondition.find()) {
            if (eqCondition.groupCount() > 1) {
                throw new UnsupportedOperationException();
            }
            List<List<String>> matches = matchPattern(EQ_OPERATOR, joinInfo);
            String[] leftJoinInfos = matches.get(0).get(1).split("\\."), rightJoinInfos = matches.get(0).get(2).split("\\.");
            leftTable = String.format("%s.%s", leftJoinInfos[0], leftJoinInfos[1]);
            rightTable = String.format("%s.%s", rightJoinInfos[0], rightJoinInfos[1]);
            List<String> leftCols = new ArrayList<>(), rightCols = new ArrayList<>();
            for (List<String> match : matches) {
                leftJoinInfos = match.get(1).split("\\.");
                rightJoinInfos = match.get(2).split("\\.");
                String currLeftTable = String.format("%s.%s", leftJoinInfos[0], leftJoinInfos[1]),
                        currLeftCol = leftJoinInfos[2],
                        currRightTable = String.format("%s.%s", rightJoinInfos[0], rightJoinInfos[1]),
                        currRightCol = rightJoinInfos[2];
                if (!leftTable.equals(currLeftTable) || !rightTable.equals(currRightTable)) {
                    throw new TouchstoneException("join中包含多个表的约束,暂不支持");
                }
                leftCols.add(currLeftCol);
                rightCols.add(currRightCol);
            }
            leftCol = String.join(",", leftCols);
            rightCol = String.join(",", rightCols);
            result[0] = leftTable;
            result[1] = leftCol;
            result[2] = rightTable;
            result[3] = rightCol;
        }
        return convertToDbTableName(result);
    }

    private String[] convertToDbTableName(String[] result) {
        if (aliasDic.containsKey(result[0])) {
            result[0] = aliasDic.get(result[0]);
        }
        if (aliasDic.containsKey(result[2])) {
            result[2] = aliasDic.get(result[2]);
        }
        return result;
    }

    @Override
    protected SelectResult analyzeSelectInfo(String operatorInfo) throws TouchstoneException {
        SelectResult result;
        try {
            result = parser.parseSelectOperatorInfo(operatorInfo);
            return result;
        } catch (Exception e) {
            String stackTrace = Throwables.getStackTraceAsString(e);
            throw new UnsupportedSelect(operatorInfo, stackTrace);
        }
    }
}
