package rm.project.map;

import rm.project.context.MultiThreadContext;
import rm.project.resource.MapResource;

public abstract class Mapper<T> implements Runnable {

    private MapperExecutor executor = null;

    private T data;

    void setExecutor(MapperExecutor executor) {
        this.executor = executor;
    }

    void setup(T data) {
        this.data = data;
    }

    protected abstract void map(T data, MultiThreadContext context);

    private MapResource resource = null;

    private MultiThreadContext context;

    void setContext(MultiThreadContext context) {
        this.context = context;
    }

    @Override
    public void run() {
        map(data, context);
        executor.batchCompleted(this);
    }
}
