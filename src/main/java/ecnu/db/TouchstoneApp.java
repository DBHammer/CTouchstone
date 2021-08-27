package ecnu.db;

import ecnu.db.analyzer.TaskConfigurator;
import ecnu.db.generator.DataGenerator;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "touchstone",
        version = {"touchstone 0.1.0",
                "JVM: ${java.version} (${java.vendor} ${java.vm.name} ${java.vm.version})",
                "OS: ${os.name} ${os.version} ${os.arch}"},
        description = "tool for generating test database", sortOptions = false,
        subcommands = {TaskConfigurator.class, DataGenerator.class},
        mixinStandardHelpOptions = true,
        commandListHeading = "Commands:\n",
        header = {
                "@|green  _____                _         _ |@",
                "@|green |_   _|__  _   _  ___| |__  ___| |_ ___  _ __   ___ |@",
                "@|green   | |/ _ \\| | | |/ __| '_ \\/ __| __/ _ \\| '_ \\ / _ \\ |@",
                "@|green   | | (_) | |_| | (__| | | \\__ \\ || (_) | | | |  __/ |@",
                "@|green   |_|\\___/ \\__,_|\\___|_| |_|___/\\__\\___/|_| |_|\\___| |@",
                ""}
)
public class TouchstoneApp implements Runnable {
    public static void main(String... args) {
        int exitCode = new CommandLine(new TouchstoneApp()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public void run() {
    }
}
