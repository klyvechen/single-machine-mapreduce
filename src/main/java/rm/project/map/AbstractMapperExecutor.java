package rm.project.map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rm.project.context.MultiThreadContext;
import rm.project.resource.MapResource;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

abstract class AbstractMapperExecutor<MKey, MValue> implements MapperExecutor<MKey, MValue>{
    Logger logger = LoggerFactory.getLogger(AbstractMapperExecutor.class);

    final MapperPool mapperPool = new MapperPool();

    MapResource<MValue> mapResource;

    MultiThreadContext context;

    private List<Observable> taskList = new LinkedList<>();

    void addTask(Observable dataToMap) {
        taskList.add(dataToMap);
    }

    void doTask(Mapper mapper) {
        if (taskList.size() == 0)
            return;
        logger.info("Appended task to execute");
        Observable taskToDo = taskList.get(0);
        if (taskToDo != null) {
            taskToDo.notifyObservers(mapper);
        }
    }

    public void mergeMaps() {
        Map<MKey, List<MValue>> result = context.getMapResultMap();
        for (Mapper mapper: mapperPool.getAllMaps()) {
            Map<MKey, List<MValue>> map = context.getRunnableMap(mapper);
            if (map == null)
                continue;
            for (MKey mkey : map.keySet()) {
                if (result.get(mkey) == null) {
                    result.put(mkey, new ArrayList<>());
                }
                result.get(mkey).addAll(map.get(mkey));
            }
        }
    }

    public void setContext(MultiThreadContext context) {
        this.context = context;
    }

    public void setResource(MapResource<MValue> mapResource) {
        this.mapResource = mapResource;
    }

    public void setMapperPool(int size, final Class<Mapper> mapperClazz) throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        for (int i = 0; i < size; i++) {
            Mapper mapper = mapperClazz.getConstructor().newInstance();
            mapper.setExecutor(this);
            mapper.setup(mapResource);
            mapper.setName("Mapper-" + i);
            this.mapperPool.idleMappers.add(mapper);
        }
        this.mapperPool.setPoolSize(size);
    }

    class MapperPool {

        private List<Mapper> idleMappers = new LinkedList<>();

        private int poolSize;

        public void setPoolSize(int size) {
            this.poolSize = size;
        }

        public int getPoolSize() {
            return poolSize;
        }

        public Mapper getAvailableMapper() {
            if (idleMappers.size() > 0) {
                Mapper mapper = idleMappers.remove(0);
                return mapper;
            } else {
                return null;
            }
        }

        public List<Mapper> getAllMaps() {
            return idleMappers;
        }

        public void recedeMapper(Mapper mapper) {
            idleMappers.add(mapper);
        }
    }
}
