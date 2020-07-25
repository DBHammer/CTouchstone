package ecnu.db.constraintchain;

import ecnu.db.constraintchain.chain.ConstraintChain;

import java.util.List;

/**
 * @author wangqingshuai
 */
public class QueryInstantiation {
    /**
     * todo 参数实例化计算
     */
    public static void compute(List<ConstraintChain> constraintChains) {
        //todo 1. 对于数值型的filter, 首先计算单元的filter, 然后计算多值的filter，
        //        对于bet操作，先记录阈值，然后选择合适的区间插入，等值约束也需选择合适的区间
        //        每个filter operation内部保存自己实例化后的结果
        //     2. 对于字符型的filter, 只有like和eq的运算，直接计算即可
    }
}
