package web;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Component;

@SpringBootApplication
//@ComponentScan("configuration")


public class WebSearchApp {

	public static void main(String[] args) {
        SpringApplication.run(WebSearchApp.class, args);
    }



}
