package ecnu.db;

import ecnu.db.generator.constraintchain.ConstraintChain;
import ecnu.db.generator.constraintchain.ConstraintChainManager;
import ecnu.db.generator.constraintchain.join.ConstraintChainFkJoinNode;
import ecnu.db.schema.Table;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.TopologicalOrderIterator;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class removeTable {

    public static void main(String[] args) throws IOException {
        File sqlFile = new File("D:\\eclipse-workspace\\Mirage\\conf\\queriesSSB");
        Map<String, String> queryName2Sql = new HashMap<>();
        read(queryName2Sql, sqlFile);
        Map<String, List<String>> sql2JoinOrder = sqlName2JoinOrder("D:\\eclipse-workspace\\Mirage\\resultSSB");
        for (Map.Entry<String, String> q2Sql : queryName2Sql.entrySet()) {
            String queryName = q2Sql.getKey();
            String sql = q2Sql.getValue();
            //转换成result中的查询文件名
            String newQueryName = queryName.split("\\.")[0] + "." + queryName.split("\\.")[1];
            List<String> joinOrder = sql2JoinOrder.get(newQueryName);
            String newSql = removeTable(sql, joinOrder);
            FileWriter fileWriter = new FileWriter("D:\\eclipse-workspace\\Mirage\\SSBInMirageWithSameJoinOrder\\ssbOrigin" + "\\" + queryName);
            fileWriter.write(newSql);
            fileWriter.close();
        }

    }

    private static void read(Map<String, String> map, File srcFolder) throws IOException {
        File[] fileArray = srcFolder.listFiles();
        assert fileArray != null;
        for (File file : fileArray) {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            StringBuilder content = new StringBuilder();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                if (!line.trim().startsWith("--")) {
                    content.append(line).append(" ");
                }
            }
            map.put(file.getName(), content.toString());
        }
    }

    public static String removeTable(String sql, List<String> joinOrder) {
        int fromStart = sql.indexOf("from");
        if (fromStart == -1) {
            fromStart = sql.indexOf("FROM");
        }
        int whereStart = sql.indexOf("where");
        if (whereStart == -1) {
            whereStart = sql.indexOf("WHERE");
        }
        String originJoin = sql.substring(fromStart+5, whereStart - 1);
        originJoin = replaceBlank(originJoin);
        List<String> tables = Arrays.stream(originJoin.split(",")).toList();
        StringBuilder joinCond = new StringBuilder();
        for (int i = 0; i < joinOrder.size(); i++) {
            String newTable = joinOrder.get(i);
            for (String table : tables) {
                if(table.contains(newTable+" ")){
                    newTable = table;
                }
            }
            joinCond.append(newTable);
            if (i != (joinOrder.size() - 1)) {
                joinCond.append(" cross join ");
            }
        }
        String from = sql.substring(0, fromStart - 1) + " from " + joinCond + " " + sql.substring(whereStart, sql.length() - 1);
        //from = from.replaceFirst("\\*", "count(*)");
        return from;
    }

    public static String replaceBlank(String str) {
        String dest = "";
        if (str != null) {
            Pattern p = Pattern.compile("\\s+|\t|\r|\n");
            Matcher m = p.matcher(str);
            dest = m.replaceAll(" ");
            Pattern p2 = Pattern.compile("cross join");
            Matcher m2 = p2.matcher(dest);
            dest = m2.replaceAll(",");
        }
        return dest;
    }

    private static Map<String, List<String>> sqlName2JoinOrder(String configPath) throws IOException {
        Map<String, List<String>> sqlName2JoinOrder = new HashMap<>();
        Map<String, List<ConstraintChain>> query2chains = ConstraintChainManager.loadConstrainChainResult(configPath);
        for (Map.Entry<String, List<ConstraintChain>> stringListEntry : query2chains.entrySet()) {
            Graph<String, DefaultEdge> graph = new DefaultDirectedGraph<>(DefaultEdge.class);
            for (ConstraintChain constraintChain : stringListEntry.getValue()) {
                graph.addVertex(constraintChain.getTableName().split("\\.")[1]);
            }
            for (ConstraintChain haveFkConstrainChain : stringListEntry.getValue()) {
                List<ConstraintChainFkJoinNode> fkJoinNodes = haveFkConstrainChain.getFkNodes();
                for (ConstraintChainFkJoinNode fkJoinNode : fkJoinNodes) {
                    graph.addEdge(fkJoinNode.getLocalCols().split("\\.")[1], fkJoinNode.getRefCols().split("\\.")[1]);
                }
            }
            List<String> joinOrder = new ArrayList<>();
            var iter = new TopologicalOrderIterator<>(graph);
            while (iter.hasNext()) {
                joinOrder.add(iter.next());
            }
            sqlName2JoinOrder.put(stringListEntry.getKey(), joinOrder);
        }
        return sqlName2JoinOrder;
    }
}

