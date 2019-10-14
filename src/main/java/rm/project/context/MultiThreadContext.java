package rm.project.context;


import java.util.HashMap;
import java.util.Map;

public class MultiThreadContext<MapKeyT, MapValuesC> {

    private Map<Runnable, Map<MapKeyT, MapValuesC>> threadMap = new HashMap<>();

    private Map<MapKeyT, MapValuesC> rootMap = new HashMap<>();

    public Map<MapKeyT, MapValuesC> getMap(Runnable runnable) {
        return threadMap.get(runnable);
    }

    public void put(Runnable runnable, Map<MapKeyT, MapValuesC> map) {
        threadMap.put(runnable, map);
    }

    public void completeMapPhase() {
        for(Map map : threadMap.values()) {
            rootMap.putAll(map);
        }
    }

    public Map<MapKeyT, MapValuesC> getOverallMap() {
        return rootMap;
    }
}
