package rm.project.map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rm.project.context.MultiThreadContext;
import rm.project.resource.MapResource;

import java.lang.reflect.InvocationTargetException;
import java.util.LinkedList;
import java.util.List;
import java.util.Observable;

public class MapperExecutor<T> {
    Logger logger = LoggerFactory.getLogger(MapperExecutor.class);

    final private MapperPool mapperPool = new MapperPool();

    private MapResource<T> mapResource;

    private MultiThreadContext context;

    public void setContext(MultiThreadContext context) {
        this.context = context;
    }

    public void setResource(MapResource<T> mapResource) {
        this.mapResource = mapResource;
    }

    public void processMultiThreadBatch() {
        if (mapResource == null) {
            logger.error("MapResource is null => return");
            return;
        }
        if (mapResource.isFinished()) {
            logger.error("MapResource finished => return");
            return;
        }
        if (mapResource.nextBatch()) {
            MapperData<T> dataToMap = new MapperData<>();
            dataToMap.setData(mapResource.getBatchData());
            dataToMap.addObserver((Observable o, Object arg) -> {
                        logger.debug("Observer is notified:" + arg );
                        Mapper<T> mapper = (Mapper) arg;
                        mapper.setup(((MapperData<T>) o).getData());
                        mapper.setContext(context);
                        Thread th = new Thread(mapper);
                        th.start();
                    }
            );
            Mapper mapper = mapperPool.getAvailableMapper();
            mapperPool.addTask(dataToMap);
            logger.debug("Mapper :" + mapper);
            if (mapper != null) {
                dataToMap.notifyObservers(mapper);
                processMultiThreadBatch();
            }
        } else {
            mapResource.setFinished();
            return;
        }
    }

    public void batchCompleted(Mapper<T> mapper) {
        logger.debug("Mapper completed: " + mapper);
        mapperPool.recedeMapper(mapper);
        mapperPool.doTask(mapper);
        processMultiThreadBatch();
    }

    public void setMapperPool(int size, final Class<Mapper> mapperClazz) throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        for (int i = 0; i < size; i++) {
            Mapper mapper = mapperClazz.getConstructor().newInstance();
            mapper.setExecutor(this);
            this.mapperPool.idleMappers.add(mapper);
        }
    }

    private class MapperPool {

        private List<Mapper> idleMappers = new LinkedList<>();

        private List<Observable> taskList = new LinkedList<>();

        public void addTask(Observable dataToMap) {
            taskList.add(dataToMap);
        }

        public Mapper getAvailableMapper() {
            if (idleMappers.size() > 0) {
                Mapper mapper = idleMappers.remove(0);
                return mapper;
            } else {
                return null;
            }
        }

        public void recedeMapper(Mapper mapper) {
            idleMappers.add(mapper);
        }

        public void doTask(Mapper mapper) {
            Observable taskToDo = taskList.get(0);
            if (taskToDo != null) {
                taskToDo.notifyObservers(mapper);
            }
        }
    }

}
