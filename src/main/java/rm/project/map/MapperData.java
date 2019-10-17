package rm.project.map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Observable;

public class MapperData<T> extends Observable {
    Logger logger = LoggerFactory.getLogger(MapperData.class);
    private T data;

    public T getData() {
        return this.data;
    }

    public void setData(T data) {
        this.data = data;
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
