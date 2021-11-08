package ecnu.db.analyzer.online.adapter.pg;

import com.jayway.jsonpath.ReadContext;
import net.minidev.json.JSONObject;

import java.util.LinkedHashMap;
import java.util.List;

public class PgJsonReader {
    private PgJsonReader() {
    }

    private static ReadContext readContext;



    static void setReadContext(ReadContext readContext) {
        PgJsonReader.readContext = readContext;
    }

    static StringBuilder skipNodes(StringBuilder path) {
        while (new PgNodeTypeInfo().isPassNode(readNodeType(path))) {//找到第一个可以处理的节点
            move2LeftChild(path);
        }
        return path;
    }

    static List<String> readGroupKey(StringBuilder path) {
        return readContext.read(path + "['Group Key']");
    }


    static int readPlansCount(StringBuilder path) {
        return readContext.read(path + "['Plans'].length()");
    }

    static boolean hasInitPlan(StringBuilder path) {
        List<String> subPlanTags = readContext.read(path + "['Plans'][*].['Subplan Name']");
        if (!subPlanTags.isEmpty()) {
            return subPlanTags.stream().anyMatch(subPlanTag -> subPlanTags.contains("InitPlan"));
        }
        return false;
    }

    static List<String> readOutput(StringBuilder path) {
        return readContext.read(path + "['Output']");
    }

    static String readPlan(StringBuilder path, int index) {
        LinkedHashMap<String, Object> data = readContext.read(path + "['Plans'][" + index + "]");
        return JSONObject.toJSONString(data);
    }

    static String readFilterInfo(StringBuilder path) {
        if ((int) readContext.read(path + "['Filter'].length()") > 0) {
            return readContext.read(path + "['Filter']");
        } else {
            return null;
        }
    }

    static String readIndexJoin(StringBuilder path) {
        return "Index Cond: " + readContext.read(path + "['Plans'][1]['Index Cond']");
    }

    static String readHashJoin(StringBuilder path) {
        StringBuilder joinInfo = new StringBuilder("Hash Cond: ").append((String) readContext.read(path + "['Hash Cond']"));
        if ((int) readContext.read(path + "['Join Filter'].length()") > 0) {
            joinInfo.append(" Join Filter: ").append((String) readContext.read(path + "['Join Filter']"));
        }
        return joinInfo.toString();
    }

    static int readRowsRemoved(StringBuilder path) {
        return readContext.read(path + "['Rows Removed by Filter']");
    }

    static StringBuilder move2LeftChild(StringBuilder path) {
        return path.append("['Plans'][0]");
    }

    static StringBuilder move2RightChild(StringBuilder path) {
        return path.append("['Plans'][1]");
    }

    static int readRowCount(StringBuilder path) {
        return readContext.read(path + "['Actual Rows']");
    }

    static String readNodeType(StringBuilder path) {
        return readContext.read(path + "['Node Type']");
    }

    static String readTableName(String path) {
        return readContext.read(path + "['Schema']") + "." + readContext.read(path + "['Relation Name']");
    }

    static String readAlias(String path) {
        return readContext.read(path + "['Alias']");
    }
}
