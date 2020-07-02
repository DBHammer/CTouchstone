package ecnu.db.dbconnector;

import ecnu.db.utils.TouchstoneToolChainException;

import java.util.List;
import java.util.Map;

public class DumpFileConnector implements DatabaseConnectorInterface {

    private final List<String> tableNames;

    private final Map<String, List<String[]>> queryPlanMap;

    private final Map<String, Integer> multiColumnsNdvMap;

    public DumpFileConnector(List<String> tableNames, Map<String, List<String[]>> queryPlanMap, Map<String, Integer> multiColumnsNdvMap) {
        this.tableNames = tableNames;
        this.queryPlanMap = queryPlanMap;
        this.multiColumnsNdvMap = multiColumnsNdvMap;
    }

    @Override
    public List<String> getTableNames() {
        return tableNames;
    }

    @Override
    public List<String[]> explainQuery(String queryCanonicalName, String sql, String[] sqlInfoColumns) throws TouchstoneToolChainException {
        List<String[]> queryPlan = this.queryPlanMap.get(String.format("%s_dump", queryCanonicalName));
        if (queryPlan == null) {
            throw new TouchstoneToolChainException(String.format("cannot find query plan for %s", queryCanonicalName));
        }
        return queryPlan;
    }

    @Override
    public int getMultiColNdv(String schema, String columns) throws TouchstoneToolChainException {
        Integer ndv = this.multiColumnsNdvMap.get(String.format("%s.%s", schema, columns));
        if (ndv == null) {
            throw new TouchstoneToolChainException(String.format("cannot find multicolumn ndv information for schema:%s, cols:%s", schema, columns));
        }
        return ndv;
    }

    @Override
    public Map<String, Integer> getMultiColNdvMap() {
        return this.multiColumnsNdvMap;
    }
}
