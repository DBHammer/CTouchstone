package ecnu.db.constraintchain.arithmetic;

/**
 * @author alan
 */
public enum ArithmeticNodeType {
    /**
     * 常数类型计算节点
     */
    CONSTANT,
    /**
     * COLUMN类型计算节点, 用于实例化
     */
    COLUMN,
    /**
     * 加类型计算节点
     */
    PLUS,
    /**
     * 减类型计算节点
     */
    MINUS,
    /**
     * 乘类型计算节点
     */
    MUL,
    /**
     * 除类型计算节点
     */
    DIV
}
