package rm.project;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rm.project.context.MultiThreadContext;
import rm.project.map.Mapper;
import rm.project.map.MapperExecutor;
import rm.project.reduce.Reducer;
import rm.project.resource.MapResource;

import java.util.HashSet;
import java.util.Observable;
import java.util.Set;


/**
 * MultiThreadMRRunner為使用多執行緒去執行Map Reduce,
 * 執行工作順序為, 從resource取得下一筆批次的資料送給Mapper去做把資料map起來, 最後將所有mapper的資料做merge, 最後進行將資料merge起來.
 * @param <MD>
 * @param <RD>
 */
public class MultiThreadMRRunner<MD extends MapResource, RD> {
    Logger logger = LoggerFactory.getLogger(MapperExecutor.class);

    private Class mapperClazz;

    private Class reducerClass;

    private MapperExecutor mapperExecutor = null;

    private Set<Mapper> idleMappers = new HashSet<Mapper>();

    private Set<Reducer> idleReducers = new HashSet<Reducer>();

    private MD dataToMap;

    private int mapperAmount;

    private int reduceAmount;

    private Observable waitForThread;

    private MapResource mapResource;

    final private MultiThreadContext context = new MultiThreadContext();

    private void setupMapperExecutor() throws NoSuchMethodException, Exception{
        logger.debug("mapperAmount: " + mapperAmount);
        logger.debug("context: " + context);
        mapperExecutor = new MapperExecutor();
        mapperExecutor.setResource(mapResource);
        mapperExecutor.setMapperPool(mapperAmount, mapperClazz);
        mapperExecutor.setContext(context);
    }

    public void execute() throws NoSuchMethodException, Exception {
        logger.info("Start to execute the runner.");
//        if (!validateProperties())
//            return;
        if (mapperExecutor == null)
            setupMapperExecutor();
        mapperExecutor.processMultiThreadBatch();
    }

    public void setMapperAmount(int mapperAmount) {
        this.mapperAmount = mapperAmount;
    }

    private boolean validateProperties() {
        return (mapperClazz != null) && (reducerClass != null) && (mapResource != null);
    }

    public MultiThreadMRRunner(Class mapper, Class reducer, MapResource resource) {
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
