package rm.project.map;

import rm.project.context.MultiThreadContext;
import rm.project.resource.MapResource;

import java.lang.reflect.InvocationTargetException;

public interface MapperExecutor<MKey, MValue> {

    void batchCompleted(Mapper<MKey, MValue> mapper);

    void processMultiThreadBatch() throws Exception;

    void setContext(MultiThreadContext context);

    void setResource(MapResource<MValue> mapResource);

    void setMapperPool(int size, final Class<Mapper> mapperClazz) throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException;

    void mergeMaps();
}
