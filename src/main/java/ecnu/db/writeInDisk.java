package ecnu.db;

import ecnu.db.analyzer.QueryInstantiate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "write", description = "write file in disk")
public class writeInDisk implements Callable<Integer> {
    @CommandLine.Option(names = {"-o", "--output"}, defaultValue = "./output", description = "the output path for dll")
    private String outputPath;

    private final Logger logger = LoggerFactory.getLogger(QueryInstantiate.class);
    public static long time = 0;

    @Override
    public Integer call() throws IOException {
        long start = System.currentTimeMillis();
        StringBuilder a = new StringBuilder();
        a.append("0".repeat(1023));
        a.append("\n");
        FileWriter fw = new FileWriter(outputPath);
        BufferedWriter bw = new BufferedWriter(fw);
        for (int i3 = 0; i3 < 1024 * 1024 * 1024; i3++) {
            bw.write(a.toString());
        }
        bw.close();
        fw.close();
        time = System.currentTimeMillis() - start;
        logger.info("time:{}", time);
        return null;
    }
}
