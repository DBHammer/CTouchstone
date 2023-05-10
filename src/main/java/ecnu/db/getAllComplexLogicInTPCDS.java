package ecnu.db;

import ecnu.db.generator.constraintchain.ConstraintChain;
import ecnu.db.generator.constraintchain.ConstraintChainManager;
import ecnu.db.generator.constraintchain.ConstraintChainNode;
import ecnu.db.generator.constraintchain.ConstraintChainNodeType;
import ecnu.db.schema.ColumnManager;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class getAllComplexLogicInTPCDS {
    public static String configPath = "resultTPCDS";
    public static void main(String[] args) throws IOException {
        ColumnManager.getInstance().setResultDir(configPath);
        ColumnManager.getInstance().loadColumnMetaData();
        ColumnManager.getInstance().loadColumnDistribution();
        Map<String, List<ConstraintChain>> query2chains = ConstraintChainManager.loadConstrainChainResult(configPath);
        for (Map.Entry<String, List<ConstraintChain>> queryChain : query2chains.entrySet()) {
            List<ConstraintChain> chain = queryChain.getValue();
            String queryName = queryChain.getKey();
            for (ConstraintChain constraintChain : chain) {
                List<ConstraintChainNode> nodes = constraintChain.getNodes();
                for (ConstraintChainNode node : nodes) {
                    if (node.getConstraintChainNodeType() == ConstraintChainNodeType.FILTER) {
                        String s = node.toString();
                        if((s.contains("and")&&s.contains("or"))){
                            System.out.println(queryName);
                            //System.out.println((queryName.substring(0, 4) + queryName.substring(6, 10)));

                            //System.out.println(s);
                        }
                    }
                }
            }
        }

    }
}
