package sample5.config;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.datasource.init.DatabasePopulator;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import ted.spring.annotation.TedDataSource;

import javax.sql.DataSource;

/**
 * @author Augustus
 * created on 2019.12.28
 *
 * when few datasources exists in project, then
 * the @TedDataSource can be used to mark one for TED,
 * or one of them should be Primary.
 *
 * annotation @TedDataSource has higher precendence than Primary
 *
 */
@Configuration
public class DataSourceConfig {

    @Primary
    @Bean(name = "postgreDataSource")
    @ConfigurationProperties(prefix = "postgre.datasource")
    public DataSource postgreDataSource() {
        return DataSourceBuilder.create().build();
    }

    @TedDataSource
    @Bean(name = "hsqldbDataSource")
    public DataSource hsqldbDataSource(Environment env) {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setDriverClassName(env.getRequiredProperty("spring.datasource.driver-class-name"));
        dataSource.setJdbcUrl(env.getRequiredProperty("spring.datasource.url"));
        dataSource.setUsername(env.getRequiredProperty("spring.datasource.username"));
        dataSource.setPassword(env.getRequiredProperty("spring.datasource.password"));

        // schema init
        Resource initSchema = new ClassPathResource("scripts/schema-hsqldb.sql");
        DatabasePopulator databasePopulator = new ResourceDatabasePopulator(initSchema);
        DatabasePopulatorUtils.execute(databasePopulator, dataSource);

        return dataSource;
    }

}
