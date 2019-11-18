package rm.project.map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rm.project.context.MultiThreadContext;
import rm.project.resource.MapResource;
import rm.project.resource.MapResourcePage;

import java.lang.reflect.InvocationTargetException;
import java.util.LinkedList;
import java.util.List;
import java.util.Observable;

public class MultiThreadResourceMapperExecutor<MKey, MValue> extends AbstractMapperExecutor<MKey, MValue> {
    Logger logger = LoggerFactory.getLogger(MultiThreadResourceMapperExecutor.class);

    Observable mainThreadObservable = new Observable();

    private final Object lock = new Object();

    Object getLock() {
        return lock;
    }

    public void processMultiThreadBatch() throws Exception {
        if (mapResource == null) {
            logger.error("MapResource is null => return");
            return;
        }
        if (mapResource.isFinished()) {
            logger.error("MapResource finished => return");
            return;
        }
        final MapResourcePage resourcePage = mapResource.nextPage();
        final Mapper mapper = mapperPool.getAvailableMapper();
        logger.debug("Mapper:" + mapper);
        if (mapper != null) {
            executeMapperWorker(mapper, resourcePage);
        } else {
            final MapperData<MValue> dataToMap = new MapperData<>();
            dataToMap.setPage(resourcePage);
            dataToMap.addObserver((Observable o, Object arg) -> {
                        executeMapperWorker((Mapper) arg, ((MapperData<MValue>) o).getPage());
                    }
            );
            addTask(dataToMap);
            logger.debug("Mapper :" + mapper);
            synchronized (getLock()) { //是否要synchronize MapperExecutor, 還是Synchronize 其他比較好??
                try {
                    logger.debug("Thread " + Thread.currentThread().getName() + " to be paused.");
                    getLock().wait(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        processMultiThreadBatch();
    }

    private void executeMapperWorker(final Mapper<MKey, MValue> mapper, final MapResourcePage<MValue> resourcePage) {
        logger.debug("Execute mapper:" + mapper);
        logger.debug("resourcePage:" + resourcePage);
        logger.debug("Execute resourcePage:" + resourcePage);
        mapper.setResourcePage(resourcePage);
        mapper.setContext(context);
        Thread th = new Thread(mapper);
        th.start();
    }

    public void batchCompleted(Mapper<MKey, MValue> mapper) {
        logger.debug("Executor: Mapper completed: " + mapper);
        mapperPool.recedeMapper(mapper);
        doTask(mapper);
        synchronized(getLock()) {
            getLock().notify();
        }
    }
}
