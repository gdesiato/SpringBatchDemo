package com.example.batch.demo.configuration;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.batch.BatchDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;

@Configuration
public class DatabaseConfiguration {
    // @Value annotation assigns the value to attribute at the time of bean creation
    @Value("${mainDatasource.driver}") private String mainDatasourceDriver;
    @Value("${mainDatasource.url}") private String mainDatasourceUrl;
    @Value("${mainDatasource.username}") private String mainDatasourceUsername;
    @Value("${mainDatasource.password}") private String mainDatasourcePassword;

    @Value("${batchDatasource.driver}") private String batchDatasourceDriver;
    @Value("${batchDatasource.url}") private String batchDatasourceUrl;
    @Value("${batchDatasource.username}") private String batchDatasourceUsername;
    @Value("${batchDatasource.password}") private String batchDatasourcePassword;

    @Bean
    @Primary
    // The @Primary annotation is used to assign in higher preference to the annotated bean
    // during bean injection when the class has multiple beans of same time,
    // in this case we have two beans of the DataSource type.
    public DataSource mainDatasource() {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName(mainDatasourceDriver);
        config.setJdbcUrl(mainDatasourceUrl);
        config.setUsername(mainDatasourceUsername);
        config.setPassword(mainDatasourcePassword);
        return new HikariDataSource(config);
    }

    @Bean
    @BatchDataSource
    // @BatchDataSource annotation is used to specify that this bean will be used as the data source
    // for the ItemReader during Batch Processing
    public DataSource batchDatasource() {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName(batchDatasourceDriver);
        config.setJdbcUrl(batchDatasourceUrl);
        config.setUsername(batchDatasourceUsername);
        config.setPassword(batchDatasourcePassword);
        return new HikariDataSource(config);
    }
}
