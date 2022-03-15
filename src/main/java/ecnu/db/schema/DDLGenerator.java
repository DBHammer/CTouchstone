package ecnu.db.schema;

import picocli.CommandLine;

import java.io.File;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "create", description = "generate the ddl sql for the new database")
public class DDLGenerator implements Callable<Integer> {
    @CommandLine.Option(names = {"-c", "--config_path"}, required = true, description = "the config path for creating ddl")
    private String configPath;
    @CommandLine.Option(names = {"-d", "--database"}, defaultValue = "TouchstoneDatabase", description = "the database name")
    private String dataBase;
    @CommandLine.Option(names = {"-o", "--output"}, defaultValue = "./ddl", description = "the output path for dll")
    private String outputPath;

    @Override
    public Integer call() {
        File configurationFile = new File(configPath);
        return null;
    }
}
