import ecnu.db.Extractor;
import ecnu.db.utils.SystemConfig;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine;

import java.util.concurrent.Callable;

/**
 * @author alan
 */

@Command(name = "touchstone", version = "touchstone 1.0",
        description = "tool for generating test database")
public class TouchstoneApp implements Callable<Integer> {
    @Option(names = {"-c", "--config_path"}, description = "file path to read configuration")
    private String configPath;



    public static void main(String... args) {
        int exitCode = new CommandLine(new TouchstoneApp()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        SystemConfig config = new SystemConfig();
        if (configPath != null) {
            config = SystemConfig.readConfig(configPath);
        }
        Extractor.extract(config);
        return 0;
    }
}
