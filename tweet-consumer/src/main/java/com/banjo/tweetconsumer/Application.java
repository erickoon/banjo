package com.banjo.tweetconsumer;

import com.banjo.tweetconsumer.service.StreamApiService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class Application {

	public static void main(String[] args) throws InterruptedException {

		ConfigurableApplicationContext ctx  = SpringApplication.run(Application.class, args);

        StreamApiService service = (StreamApiService)ctx.getBean("streamApiService");
        service.start();

    }
}
