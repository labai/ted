package sample3.job;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import ted.driver.TedDriver;
import ted.driver.TedResult;
import ted.driver.TedTask;

import javax.annotation.PostConstruct;
import java.lang.reflect.ParameterizedType;

/**
 * @author Augustus
 *         created on 2018.08.25
 *
 * sample AbstractTedProcessor.
 *
 * This TedProcessor is single component, thus do not hold state here.
 *
 */
abstract class AbstractTedProcessor<T> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractTedProcessor.class);

    @Autowired
    private TedDriver tedDriver;

    @Autowired
    private Gson gson;

    private final String taskName;
    private final Class<T> clazz;

    /** task processor.
     * data - data object parsed from json
     */
    abstract protected TedResult processTask(TedTask task, T data);


    /** can handle timeouts.
     * This method will be called before main task processing.
     * If returns null - then will continue and processTask will be called,
     * otherwise, if some TedResult will be returned, it will be set to task in db.
     */
    protected TedResult onAfterTimeout(TedTask task) {
        return null;
    };

    AbstractTedProcessor(String taskName) {
        this.taskName = taskName;
        this.clazz = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    @PostConstruct
    private void registerTask() {
        tedDriver.registerTaskConfig(taskName, s -> this::processInternal);
    }

    private TedResult processInternal(TedTask task) {
        if (task.isAfterTimeout()) {
            TedResult res = onAfterTimeout(task);
            if (res != null)
                return res;
        }
        T data = jsonToData(task.getData());
        return processTask(task, data);
    }

    private T jsonToData(String json) {
        return gson.fromJson(json, this.clazz);
    }

}
