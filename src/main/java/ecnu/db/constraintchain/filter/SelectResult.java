package ecnu.db.constraintchain.filter;

import ecnu.db.constraintchain.filter.logical.AndNode;

import java.util.List;
import java.util.Set;

/**
 * @author alan
 */
public class SelectResult {
    private AndNode condition;
    private List<Parameter> parameters;
    private String tableName;
    private Set<String> columns;

    public AndNode getCondition() {
        return condition;
    }

    public void setCondition(AndNode condition) {
        this.condition = condition;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    public void setParameters(List<Parameter> parameters) {
        this.parameters = parameters;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Set<String> getColumns() {
        return columns;
    }

    public void setColumns(Set<String> columns) {
        this.columns = columns;
    }
}
