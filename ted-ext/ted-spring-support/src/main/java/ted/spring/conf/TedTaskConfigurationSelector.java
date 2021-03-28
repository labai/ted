package ted.spring.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.ImportSelector;
import org.springframework.core.type.AnnotationMetadata;

/**
 * @author Augustus
 *         created on 2019.12.04
 *
 * for TED internal usage only!!!
 *
 */
public class TedTaskConfigurationSelector implements ImportSelector {
    private static final Logger logger = LoggerFactory.getLogger(TedTaskConfigurationSelector.class);

    @Override
    public String[] selectImports(AnnotationMetadata metadata) {
        return new String[]{
            TedDriverConfiguration.class.getName()
        };

    }
}
