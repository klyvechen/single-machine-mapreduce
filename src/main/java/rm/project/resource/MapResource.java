package rm.project.resource;

/**
 * MapResource工作為從Resource取得應該計算的資源
 * @param <T>
 */
public interface MapResource<T> {

    boolean nextBatch();

    boolean isFinished();

    void setFinished();

    T getBatchData();

}
