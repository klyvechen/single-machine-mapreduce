package rm.project.map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rm.project.resource.MapResourcePage;

import java.util.Observable;

public class SingleThreadResourceMapperExecutor<MKey, MValue> extends AbstractMapperExecutor<MKey, MValue> {
    Logger logger = LoggerFactory.getLogger(SingleThreadResourceMapperExecutor.class);
    private Observable mappersReceded = new Observable();

    private Object lock = new Object();

    public void processMultiThreadBatch() throws Exception{
        if (mapResource == null) {
            logger.error("MapResource is null => return");
            return;
        }
        if (mapResource.isFinished()) {
            logger.error("MapResource finished => return");
            return;
        }
        logger.info("Start to processMultiThreadBatch in SingleThreadMapperExecutor");
        for(;!mapResource.isFinished();) {
            mapResourcePage(mapResource.nextPage());
        }
        synchronized (lock) {
            if (mapperPool.getPoolSize() != mapperPool.getAllMaps().size()) {
                try {
                    logger.info("Not all mappers are receded yet, lock the lock");
                    lock.wait(20000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void mapResourcePage(final MapResourcePage<MValue> resourcePage) {
        if (resourcePage == null)
            return;
        Mapper mapper = mapperPool.getAvailableMapper();
        if (mapper == null) {
            //execute when they are available mapper.
            logger.info("Mapper is null, do it in the future");
            final MapperData<MValue> dataToMap = new MapperData<>();
            dataToMap.setPage(resourcePage);
            dataToMap.addObserver((Observable o, Object arg) -> {
                        logger.info("Observer: execute the mapper worker.");
                        executeMapperWorker((Mapper) arg, ((MapperData<MValue>) o).getPage());
                    }
            );
            addTask(dataToMap);
        } else {
            logger.info("execute the mapper worker.");
            executeMapperWorker(mapper, resourcePage);
        }
    }

    private void executeMapperWorker(final Mapper<MKey, MValue> mapper, final MapResourcePage<MValue> resourcePage) {
        logger.debug("Execute executeMapperWorker: " + mapper.getName());
        mapper.setResourcePage(resourcePage);
        mapper.setContext(context);
        Thread th = new Thread(mapper);
        th.start();
    }

    @Override
    public void batchCompleted(Mapper<MKey, MValue> mapper) {
        logger.debug("Executor: Mapper completed: " + mapper);
        mapperPool.recedeMapper(mapper);
        doTask(mapper);
        if (mapResource.isFinished() && mapperPool.getAllMaps().size() == mapperPool.getPoolSize())  {
            synchronized(lock) {
                logger.info("All mappers are receded, unlock the lock");
                lock.notify();
            }
        }
    }
}
