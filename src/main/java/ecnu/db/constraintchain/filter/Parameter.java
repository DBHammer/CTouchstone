package ecnu.db.constraintchain.filter;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import ecnu.db.constraintchain.filter.operation.CompareOperator;

/**
 * @author alan
 * 代表需要实例化的参数
 */
@JsonIdentityInfo(generator = ObjectIdGenerators.PropertyGenerator.class, resolver = ParameterResolver.class, property = "id", scope = Parameter.class)
public class Parameter {
    /**
     * parameter的id，用于后续实例化
     */
    private Integer id;
    /**
     * parameter的data
     */
    private String data;
    /**
     * 在SQL中是否需要加"'"符号，如字符串类型
     */
    private boolean needQuote;
    /**
     * 是否是Date类型的参数
     */
    private boolean isDate;
    /**
     * 操作符
     */
    private CompareOperator operator;
    /**
     * 操作数
     */
    private String operand;

    public Parameter() {
    }

    public Parameter(Integer id, String data, boolean needQuote, boolean isDate, CompareOperator operator, String operand) {
        this.id = id;
        this.data = data;
        this.needQuote = needQuote;
        this.isDate = isDate;
        this.operator = operator;
        this.operand = operand;
    }


    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public boolean isNeedQuote() {
        return needQuote;
    }

    @JsonIgnore
    public void setNeedQuote(boolean needQuote) {
        this.needQuote = needQuote;
    }

    public boolean getIsDate() {
        return isDate;
    }

    public void setIsDate(boolean date) {
        isDate = date;
    }

    @JsonIgnore
    public CompareOperator getOperator() {
        return operator;
    }

    public void setOperator(CompareOperator operator) {
        this.operator = operator;
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
        return String.format("{id:%d, data:%s}", id, needQuote ? String.format("'%s'", data) : data);
    }
}
