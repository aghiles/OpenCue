package com.imageworks.spcue.config;

import org.springframework.context.annotation.*;

@Configuration
public class DaoConfig {
    
    /**
     * Component scan for Redis DAOs when redis profile is active
     * This allows Spring to find and autowire all Redis DAO implementations
     */
    @Configuration
    @Profile("redis")
    @ComponentScan(basePackages = "com.imageworks.spcue.dao.redis")
    public static class RedisDaoConfig {
        // The @ComponentScan will automatically find all @Repository annotated
        // Redis DAO classes and register them as beans
        // No need to manually define each bean since they're already
        // annotated with @Repository and @Profile("redis")
    }
    
    /**
     * Component scan for PostgreSQL DAOs when redis profile is NOT active
     * This is the default behavior
     */
    @Configuration
    @Profile("!redis")
    @ComponentScan(basePackages = "com.imageworks.spcue.dao.postgres")
    public static class PostgresDaoConfig {
        // Similarly, this will find all PostgreSQL DAO implementations
    }
}
