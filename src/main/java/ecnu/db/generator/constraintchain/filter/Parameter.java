package ecnu.db.generator.constraintchain.filter;

import com.fasterxml.jackson.annotation.JsonIgnore;
import ecnu.db.schema.ColumnManager;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author alan
 * 代表需要实例化的参数
 */
public class Parameter {

    private static final Pattern CanonicalColumnName = Pattern.compile("[a-zA-Z][a-zA-Z0-9$_]*\\.[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+");

    /**
     * parameter的id，用于后续实例化
     */
    private int id;
    /**
     * parameter的内部data，用于快速计算
     */
    private long data;

    private ParameterType type = ParameterType.ACTUAL;


    public boolean isEqualPredicate() {
        return isEqualPredicate;
    }

    public void setEqualPredicate(boolean equalPredicate) {
        isEqualPredicate = equalPredicate;
    }

    @JsonIgnore
    private boolean isEqualPredicate = false;

    /**
     * 操作数
     */
    @JsonIgnore
    private String operand;
    /**
     * String化的值
     */
    @JsonIgnore
    private String dataValue;

    @JsonIgnore
    private boolean isSubPlan = false;

    public Parameter() {
    }

    public Parameter(Integer id, String operand, String dataValue) {
        this.id = id;
        this.operand = operand;
        if (operand != null) {
            Matcher matcher = CanonicalColumnName.matcher(operand);
            List<String> cols = new ArrayList<>();
            if (matcher.find()) {
                cols.add(matcher.group());
            }
            if (cols.size() == 1 && ColumnManager.getInstance().isDateColumn((cols.get(0)))) {
                dataValue = dataValue.split(" ")[0];
            }
        }

        this.dataValue = dataValue;
    }

    public ParameterType getType() {
        return type;
    }

    public void setType(ParameterType type) {
        this.type = type;
    }

    public List<String> hasOnlyOneColumn() {
        if (operand == null) {
            return new LinkedList<>();
        }
        Matcher matcher = CanonicalColumnName.matcher(operand);
        List<String> cols = new ArrayList<>();
        while (matcher.find()) {
            cols.add(matcher.group());
        }
        return cols;
    }

    public Integer getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    @JsonIgnore
    public String getRealDataValue() {
        if (type == ParameterType.SUBSTRING) {
            return dataValue.replace("%", "");
        } else {
            return dataValue;
        }
    }

    public String getDataValue() {
        return dataValue;
    }

    public void setDataValue(String dataValue) {
        if (type == ParameterType.SUBSTRING) {
            dataValue = dataValue.replace("%", "");
            int length = this.dataValue.replace("%", "").length();
            dataValue = dataValue.substring(0, length);
        }
        this.dataValue = dataValue;
    }

    public long getData() {
        return data;
    }


    public void setData(long data) {
        this.data = data;
    }

    @JsonIgnore
    public String getOperand() {
        return operand;
    }

    public void setOperand(String operand) {
        this.operand = operand;
    }

    @Override
    public String toString() {
        return "{id:" + id + ", data:" + dataValue + "}";
    }

    @JsonIgnore
    public boolean isSubPlan() {
        return isSubPlan;
    }

    @JsonIgnore
    public void setSubPlan(boolean subPlan) {
        isSubPlan = subPlan;
    }

    public enum ParameterType {
        ACTUAL,
        VIRTUAL,
        LIKE,
        SUBSTRING
    }
}
