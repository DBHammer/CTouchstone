package ecnu.db.exception;

/**
 * @author alan
 */
public class UnsupportedJoin extends TouchstoneException {
    public UnsupportedJoin(String operatorInfo) {
        super(String.format("暂时不支持的join类型 operator_info:'%s'", operatorInfo));
    }
}
