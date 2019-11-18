package rm.project.map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rm.project.context.MultiThreadContext;
import rm.project.resource.MapResource;
import rm.project.resource.MapResourcePage;

import java.util.List;

public abstract class Mapper<MKey, MValue> implements Runnable {
    Logger logger = LoggerFactory.getLogger(Mapper.class);

    private String name;

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    private MapperExecutor executor = null;

    void setExecutor(MapperExecutor executor) {
        this.executor = executor;
    }

    private MapResourcePage<MValue> resourcePage = null;

    private MapResource<MValue> mapResource = null;

    void setResourcePage(MapResourcePage resourcePage) {
        this.resourcePage = resourcePage;
    }

    void setup(MapResource<MValue> mapResource) {
        this.mapResource = mapResource;
    }

    protected abstract void map(List<MValue> data, MultiThreadContext context);

    private MapResource resource = null;

    private MultiThreadContext context;

    void setContext(MultiThreadContext context) {
        this.context = context;
    }

    @Override
    public void run() {
        final List<MValue> data = resourcePage.getData();
        if (data.size() == 0) {
            mapResource.setFinished();
        } else {
            map(data, context);
            logger.debug("Executor to be notify");
        }
        executor.batchCompleted(this);
    }
}
