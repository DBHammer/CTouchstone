package ecnu.db.analyzer.online;

/**
 * @author lianxuechao
 */
public class QueryInfo {
    String data;
    String tableName;
    int lastNodeLineCount;

    public QueryInfo(String data, String tableName, int lastNodeLineCount) {
        this.data = data;
        this.lastNodeLineCount = lastNodeLineCount;
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getData() {
        return data;
    }

    public void addConstraint(String data) {
        this.data += data;
    }

    public int getLastNodeLineCount() {
        return lastNodeLineCount;
    }

    public void setLastNodeLineCount(int lastNodeLineCount) {
        this.lastNodeLineCount = lastNodeLineCount;
    }

    @Override
    public String toString() {
        return "QueryInfo{" +
                "data='" + data + '\'' +
                ", tableName='" + tableName + '\'' +
                '}';
    }
}
