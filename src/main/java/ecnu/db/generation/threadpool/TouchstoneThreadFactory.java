package ecnu.db.generation.threadpool;


import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * @author wangqingshuai
 */
public class TouchstoneThreadFactory implements java.util.concurrent.ThreadFactory {
    private final String name;
    private int counter;

    public TouchstoneThreadFactory(String name) {
        this.name = name;
        this.counter = 0;
    }

    @Override
    public Thread newThread(@NonNull Runnable r) {
        return new Thread(r, name + "-Thread-" + counter++);
    }
}