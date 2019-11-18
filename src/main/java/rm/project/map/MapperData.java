package rm.project.map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rm.project.resource.MapResourcePage;

import java.util.Observable;

public class MapperData<T> extends Observable {
    Logger logger = LoggerFactory.getLogger(MapperData.class);
    private MapResourcePage<T> page;

    public MapResourcePage<T> getPage() {
        return this.page;
    }

    public void setPage(MapResourcePage<T> page) {
        this.page = page;
    }

    @Override
    public void notifyObservers() {
        setChanged();
        super.notifyObservers();
    }

    @Override
    public void notifyObservers(Object o) {
        setChanged();
        super.notifyObservers(o);
    }
}
