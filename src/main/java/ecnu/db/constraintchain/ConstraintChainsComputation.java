package ecnu.db.constraintchain;

public class ConstraintChainsComputation {

//    /**
//     * 准备好生成tuple
//     *
//     * @param pkStart 需要生成的数据起始
//     * @param range   需要生成的数据范围
//     * @param chains  约束链条
//     * @throws TouchstoneException 生成失败
//     */
//    public static JoinInfoTable computeFksAndPkJoinInfo(int pkStart, int range, Collection<ConstraintChain> chains)
//            throws TouchstoneException {
//        Map<Integer, boolean[]> pkBitMap = new HashMap<>();
//        Table<String, Integer, boolean[]> fkBitMap = HashBasedTable.create();
//        for (ConstraintChain chain : chains) {
//            chain.evaluate(range, pkBitMap, fkBitMap);
//        }
//        for (Map.Entry<String, Map<ConstraintChainFkJoinNode, boolean[]>> entry : fkBitMap.rowMap().entrySet()) {
//            Map<ConstraintChainFkJoinNode, boolean[]> fkBitMap4Join = entry.getValue();
//            List<ConstraintChainFkJoinNode> nodes = new ArrayList<>(fkBitMap4Join.keySet()).stream()
//                    .sorted(Comparator.comparingInt(ConstraintChainFkJoinNode::getPkTag)).collect(Collectors.toList());
//            for (int i = 0; i < size; i++) {
//                long bitMap = 1L;
//                for (ConstraintChainFkJoinNode node : nodes) {
//                    bitMap = (fkBitMap4Join.get(node)[i] ? 1L : 0L) & (bitMap << 1);
//                }
//            }
//        }
//    }
}
