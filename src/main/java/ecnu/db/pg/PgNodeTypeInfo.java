package ecnu.db.pg;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

public class PgNodeTypeInfo {
    public static final HashSet<String> PASS_NODE_TYPES = new HashSet<>(Arrays.asList("Sort", "Aggregate", "Gather", "Limit", "Gather Merge"));
    public static final HashSet<String> JOIN_NODE_TYPES = new HashSet<>(Arrays.asList("Hash Join", "Nested Loop"));
    public static final HashSet<String> FILTER_NODE_TYPES = new HashSet<>(Arrays.asList("Seq Scan", "Index Scan"));

    public PgNodeTypeInfo() {}

    public boolean isPassNode(String nodeType) {
        return PASS_NODE_TYPES.contains(nodeType);
    }

    public boolean isJoinNode(String nodeType) {
        return JOIN_NODE_TYPES.contains(nodeType);
    }

    public boolean isFilterNode(String nodeType) {
        return FILTER_NODE_TYPES.contains(nodeType);
    }
}
