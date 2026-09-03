package dev.thehenrylabs.springbatchcronjob;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class SpringBatchCronjobApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringBatchCronjobApplication.class, args);
	}

}
