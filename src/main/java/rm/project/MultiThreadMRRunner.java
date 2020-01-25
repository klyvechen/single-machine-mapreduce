package rm.project;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rm.project.context.MultiThreadContext;
import rm.project.map.Mapper;
import rm.project.map.MapperExecutor;
import rm.project.map.SingleThreadResourceMapperExecutor;
import rm.project.reduce.Reducer;
import rm.project.resource.MapResource;

import java.util.List;
import java.util.Map;
import java.util.Observable;


/**
 * MultiThreadMRRunner為使用多執行緒去執行Map Reduce,
 * 執行工作順序為, 從resource取得下一筆批次的資料送給Mapper去做把資料map起來, 最後將所有mapper的資料做merge, 最後進行將資料merge起來.
 * @param <MKey>
 * @param <MValue>
 * @param <RKey>
 * @param <RValue>
 */
public class MultiThreadMRRunner<MKey, MValue, RKey, RValue> {
    Logger logger = LoggerFactory.getLogger(MultiThreadMRRunner.class);

    private Class<Mapper> mapperClazz;

    private Class<Reducer> reducerClass;

    private MapperExecutor mapperExecutor = null;

    private int mapperAmount;

    private MapResource mapResource;

    private Reducer<MKey, MValue, RKey,RValue> reducer;

    final private MultiThreadContext context = new MultiThreadContext();

    public MultiThreadContext<MKey, MValue, RKey, RValue> getContext() {
        return context;
    }

    public void init() throws NoSuchMethodException, Exception {
        reducer = reducerClass.getConstructor().newInstance();
    }

    public Reducer<MKey, MValue, RKey,RValue> getReducer() {
        return reducer;
    }

    private void setupMapperExecutor() throws NoSuchMethodException, Exception {
        logger.debug("mapperAmount: " + mapperAmount);
        logger.debug("context: " + context);
        mapperExecutor = new SingleThreadResourceMapperExecutor();
        mapperExecutor.setResource(mapResource);
        mapperExecutor.setMapperPool(mapperAmount, mapperClazz);
        mapperExecutor.setContext(context);
    }

    public void execute() throws NoSuchMethodException, Exception {
        logger.info("Start to execute the runner.");
        if (!validateProperties())
            return;
        if (mapperExecutor == null)
            setupMapperExecutor();
        mapperExecutor.processMultiThreadBatch();
        mapperExecutor.mergeMaps();
        logger.info("map result key size" + context.getMapResultMap().keySet().size());
        Map<MKey, List<MValue>> reduceMap = context.getMapResultMap();
        int i = 0;
        for (MKey key: reduceMap.keySet()) {
            reducer.reduce(key, reduceMap.get(key), context);
            if (++i % 10000 == 0) {
                logger.info(i + " items is reduced!");
            }
        }
        logger.info("Map reduce completed!");
    }

    public void setMapperAmount(int mapperAmount) {
        this.mapperAmount = mapperAmount;
    }

    private boolean validateProperties() {
        return (mapperClazz != null) && (reducerClass != null) && (mapResource != null);
    }

    public MultiThreadMRRunner(Class<Mapper> mapper, Class<Reducer> reducer, MapResource resource) {
        this.mapperClazz = mapper;
        this.reducerClass = reducer;
        this.mapResource = resource;
    }

    public void setResource(MapResource mapResource) {
        this.mapResource = mapResource;
    }

    public static MultiThreadMRRunnerBuilder builder() {
        return new MultiThreadMRRunnerBuilder();
    }

    public static class MultiThreadMRRunnerBuilder {

        private Class mapperClazz = null;

        private Class reducerClass = null;

        private MapResource resource= null;

        public MultiThreadMRRunnerBuilder mapperClazz(Class mapperClazz) {
            this.mapperClazz = mapperClazz;
            return this;
        }

        public MultiThreadMRRunnerBuilder reducerClass(Class reducerClass){
            this.reducerClass = reducerClass;
            return this;
        }

        public MultiThreadMRRunnerBuilder mapResource(MapResource resource){
            this.resource = resource;
            return this;
        }

        public MultiThreadMRRunner build() {
            return new MultiThreadMRRunner(mapperClazz, reducerClass, resource);
        }
    }
}
