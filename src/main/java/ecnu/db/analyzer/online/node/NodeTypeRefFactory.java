package ecnu.db.analyzer.online.node;

/**
 * @author lianxuechao
 */
public class NodeTypeRefFactory {
    public static NodeTypeTool getNodeTypeRef(String tidbVersion) {
        return new TidbNodeTypeTool();
    }
}
