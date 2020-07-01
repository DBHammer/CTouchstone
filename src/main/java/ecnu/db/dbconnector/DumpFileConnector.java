package ecnu.db.dbconnector;

import ecnu.db.analyzer.statical.QueryAliasParser;
import ecnu.db.utils.TouchstoneToolChainException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class DumpFileConnector implements DatabaseConnectorInterface {

    private static final Logger LOGGER = LoggerFactory.getLogger(DumpFileConnector.class);

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
            LOGGER.warn("cannot find query plan for %s", queryCanonicalName);
        }
        return queryPlan;
    }

    @Override
    public int getMultiColNdv(String schema, String columns) throws TouchstoneToolChainException {
        Integer ndv = this.multiColumnsNdvMap.get(String.format("%s.%s", schema, columns));
        if (ndv == null) {
            LOGGER.warn(String.format("cannot find multicolumn ndv information for schema:%s, cols:%s", schema, columns));
        }
        return ndv;
    }

    @Override
    public Map<String, Integer> getMultiColNdvMap() {
        return this.multiColumnsNdvMap;
    }
}
