package com.hyw.gdata.autoconfig;

import com.hyw.gdata.DataService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 用于spring加载bean
 */
@Configuration
public class DataServiceConfig {

    @Bean
    public DataService genDataServiceBean(){
        return new DataService();
    }
}
