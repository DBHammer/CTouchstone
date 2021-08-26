package ecnu.db.pg;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class PgNodeTypeInfo {
    protected static final Set<String> PASS_NODE_TYPES = new HashSet<>(Arrays.asList("Sort", "Aggregate", "Gather", "Limit", "Gather Merge"));
    protected static final Set<String> JOIN_NODE_TYPES = new HashSet<>(Arrays.asList("Hash Join", "Nested Loop"));
    protected static final Set<String> FILTER_NODE_TYPES = new HashSet<>(Arrays.asList("Seq Scan", "Index Scan"));

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
