package ecnu.db.generator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DataWriter {

    String outputPath;
    int generatorId;

    ExecutorService executorService = Executors.newSingleThreadExecutor();

    public DataWriter(String outputPath, int generatorId) {
        this.outputPath = outputPath;
        this.generatorId = generatorId;
    }

    public void addWriteTask(String schemaName, List<StringBuilder> rowData) {
        executorService.submit(() -> {
            try {
                Files.write(Paths.get(outputPath + "/" + schemaName + generatorId), rowData,
                        StandardOpenOption.APPEND, StandardOpenOption.CREATE);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public void reset() {
        executorService = Executors.newSingleThreadExecutor();
    }

    public boolean waitWriteFinish() throws InterruptedException {
        executorService.shutdown();
        return executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }

}
