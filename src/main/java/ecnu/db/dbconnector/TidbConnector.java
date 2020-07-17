package ecnu.db.dbconnector;

import ecnu.db.utils.SystemConfig;
import ecnu.db.utils.TouchstoneToolChainException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/**
 * @author wangqingshuai
 */
public class TidbConnector extends AbstractDbConnector {

    private static final Logger logger = LoggerFactory.getLogger(TidbConnector.class);
    String statsUrl;

    public TidbConnector(SystemConfig config) throws TouchstoneToolChainException {
        super(config);
        statsUrl = "http://" + config.getDatabaseIp() + ':' + config.getTidbHttpPort() + "/stats/dump/";
    }

    @Override
    String dbUrl(SystemConfig config) {
        if (!config.isCrossMultiDatabase()) {
            return "jdbc:mysql://" +
                    config.getDatabaseIp() + ":" +
                    config.getDatabasePort() + "/" +
                    config.getDatabaseName() +
                    "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
        } else {
            return "jdbc:mysql://" +
                    config.getDatabaseIp() + ":" +
                    config.getDatabasePort() +
                    "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
        }
    }

    @Override
    String abstractGetTableNames() {
        return "show tables;";
    }

    @Override
    String abstractGetCreateTableSql(String tableName) {
        return "show create table " + tableName;
    }

    public String tableInfoJson(String tableName) throws IOException {
        String tableStatsUrl = statsUrl + tableName.replace(".", "/");
        logger.info(String.format("表%s的统计数据url %s", tableName, tableStatsUrl));
        InputStream is = new URL(tableStatsUrl).openStream();
        BufferedReader rd = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
        String line;
        StringBuilder json = new StringBuilder();
        while ((line = rd.readLine()) != null) {
            json.append(line);
        }
        is.close();
        return json.toString();
    }
}
