package rm.project.reduce;

import rm.project.context.MultiThreadContext;

import java.util.List;

public interface Reducer<MKey, MValue, RKey, RValue> {
    void reduce(MKey key, List<MValue> value, MultiThreadContext<MKey ,MValue , RKey, RValue> context);
}
