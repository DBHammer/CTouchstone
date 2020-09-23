package ecnu.db.dbconnector;

import ecnu.db.exception.TouchstoneToolChainException;

import java.util.List;
import java.util.Map;

import static ecnu.db.utils.CommonUtils.DUMP_FILE_POSTFIX;

public class DumpFileConnector implements DatabaseConnectorInterface {

    private final Map<String, List<String[]>> queryPlanMap;

    private final Map<String, Integer> multiColumnsNdvMap;

    public DumpFileConnector(Map<String, List<String[]>> queryPlanMap, Map<String, Integer> multiColumnsNdvMap) {
        this.queryPlanMap = queryPlanMap;
        this.multiColumnsNdvMap = multiColumnsNdvMap;
    }


    @Override
    public List<String[]> explainQuery(String queryCanonicalName, String sql, String[] sqlInfoColumns) throws TouchstoneToolChainException {
        List<String[]> queryPlan = this.queryPlanMap.get(String.format("%s.%s", queryCanonicalName, DUMP_FILE_POSTFIX));
        if (queryPlan == null) {
            throw new TouchstoneToolChainException(String.format("cannot find query plan for %s", queryCanonicalName));
        }
        return queryPlan;
    }

    @Override
    public int getMultiColNdv(String canonicalTableName, String columns) throws TouchstoneToolChainException {
        Integer ndv = this.multiColumnsNdvMap.get(String.format("%s.%s", canonicalTableName, columns));
        if (ndv == null) {
            throw new TouchstoneToolChainException(String.format("cannot find multicolumn ndv information for schema:%s, cols:%s", canonicalTableName, columns));
        }
        return ndv;
    }

    @Override
    public Map<String, Integer> getMultiColNdvMap() {
        return this.multiColumnsNdvMap;
    }
}
