package dev.thehenrylabs.springbatchcronjob;

import org.springframework.boot.SpringApplication;

public class TestSpringBatchCronjobApplication {

	public static void main(String[] args) {
		SpringApplication.from(SpringBatchCronjobApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
