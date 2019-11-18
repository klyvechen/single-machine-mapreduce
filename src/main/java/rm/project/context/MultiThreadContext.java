package rm.project.context;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultiThreadContext<MapKey, MapValue, ReduceKey, ReduceValue> {

    private Map<Runnable, Map<MapKey, MapValue>> threadMap = new HashMap<>();

    private Map<MapKey, List<MapValue>> rootMap = new HashMap<>();

    private Map<ReduceKey, ReduceValue> reduceResultMap = new HashMap<>();

    public Map<ReduceKey, ReduceValue> getReduceValue() {
        return reduceResultMap;
    }

    public Map<MapKey, MapValue> getRunnableMap(Runnable runnable) {
        return threadMap.get(runnable);
    }

    public void put(Runnable runnable, Map<MapKey, MapValue> map) {
        threadMap.put(runnable, map);
    }

    public void completeMapPhase() {
        for(Map map : threadMap.values()) {
            rootMap.putAll(map);
        }
    }

    public Map<MapKey, List<MapValue>> getMapResultMap() {
        return rootMap;
    }

    public void write(ReduceKey key, ReduceValue value) {
        reduceResultMap.put(key, value);
    }
}
