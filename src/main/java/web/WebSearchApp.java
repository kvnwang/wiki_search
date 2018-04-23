package web;

import org.springframework.boot.CommandLineRunner; 
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
@SpringBootApplication
@ComponentScan (basePackages = {"query"})
public class WebSearchApp {

	public static void main(String[] args) {

        SpringApplication.run(WebSearchApp.class, args);
    }

	

}
