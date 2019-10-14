package rm.project.map;

import java.util.Observable;

public class MapperData<T> extends Observable {
    private T data;

    public T getData() {
        return this.data;
    }

    public void setData(T data) {
        this.data = data;
    }
}
