package com.example.springbatchchunk.config;

import com.zaxxer.hikari.HikariDataSource;
import jakarta.transaction.TransactionManager;
import jakarta.transaction.UserTransaction;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.jta.JtaTransactionManager;

import javax.sql.DataSource;

@Configuration
public class AppConfig {
    @ConfigurationProperties("spring.datasource.one.hikari")
    @Bean
    public DataSource dataSourceOne(){
        return DataSourceBuilder.create().type(HikariDataSource.class).build();
    }

    @ConfigurationProperties("spring.datasource.two.hikari")
    @Bean
    public DataSource dataSourceTwo(){
        return DataSourceBuilder.create().type(HikariDataSource.class).build();
    }


    @Bean
    public PlatformTransactionManager transactionManagerOne(@Qualifier("dataSourceOne") DataSource dataSourceOne){
        return new JdbcTransactionManager(dataSourceOne);
    }

    @Bean
    public PlatformTransactionManager transactionManagerTwo(@Qualifier("dataSourceTwo") DataSource dataSourceTwo){
        return new JdbcTransactionManager(dataSourceTwo);
    }



}
