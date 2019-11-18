package rm.project.resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class MultiThreadMapResource<T> implements MapResource<T>{
    Logger logger = LoggerFactory.getLogger(MultiThreadMapResource.class);
    abstract public MapResourcePage<T> nextPage();

    abstract public List<T> read(MapResourcePage resourcePage);

    private MapResourcePage<T> resourcePage;

    private boolean finished = false;

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public void setFinished() {
        finished = true;
    }
}
