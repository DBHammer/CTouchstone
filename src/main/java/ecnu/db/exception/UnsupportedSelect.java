package ecnu.db.exception;

/**
 * @author alan
 */
public class UnsupportedSelect extends TouchstoneToolChainException {
    public UnsupportedSelect(String operatorInfo, String stackTrace) {
        super(String.format("暂时不支持的select类型 operator_info:'%s'\n%s", operatorInfo, stackTrace));
    }
}
