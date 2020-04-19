package ecnu.db.dbconnector;

import ecnu.db.utils.SystemConfig;
import ecnu.db.utils.TouchstoneToolChainException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class TidbConnector extends AbstractDbConnector {
    String statsUrl;

    public TidbConnector(SystemConfig config) throws TouchstoneToolChainException {
        super(config);
        statsUrl = "http://" + config.getDatabaseIp() + ':' + config.getTidbHttpPort() + "/stats/dump/";
        if (!config.isCrossMultiDatabase()) {
            statsUrl += config.getDatabaseName() + "/";
        }
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
        InputStream is = new URL(statsUrl + tableName.replace(".", "/")).openStream();
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
