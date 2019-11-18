package rm.project.resource;

import java.util.List;

/**
 * MapResource工作為從Resource取得應該計算的資源
 * @param <T>
 */
public interface MapResource<T> {

    MapResourcePage<T> nextPage();

    boolean isFinished();

    void setFinished();

    List<T> read(MapResourcePage<T> resourcePage);
}
