package com.bise.simpleETL.hbase.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.context.annotation.Bean;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * Created with IDEA
 *
 * @author:Bise
 * @date:2021/4/2 19:56
 */
@EnableSwagger2
@SpringBootConfiguration
public class SwaggerConfig {
    @Value("#{'prod'.equals('${spring.profiles.active}')}")
    private boolean disableSwagger;
    @Bean
    public Docket createRestApi() {
        return new Docket(DocumentationType.SWAGGER_2)
                .enable(!disableSwagger)
                .apiInfo(apiInfo())
                .select()
                .apis(RequestHandlerSelectors.basePackage("com.bise.simpleETL"))
                .paths(PathSelectors.any())
                .build();
                //.globalOperationParameters(setHeaderToken());
    }

    private ApiInfo apiInfo() {
        return new ApiInfoBuilder()
                .title("HBaseETLAPI")
                .description("©2021 Copyright. Powered By Bise")
                .build();
    }
}
