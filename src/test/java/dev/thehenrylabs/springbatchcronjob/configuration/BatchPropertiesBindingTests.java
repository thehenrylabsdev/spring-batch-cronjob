package dev.thehenrylabs.springbatchcronjob.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import dev.thehenrylabs.springbatchcronjob.TestcontainersConfiguration;

/**
 * Verifies that the reference defaults in {@code application.yaml} bind to
 * {@link BatchProperties} (SPEC 43) — no hidden clobbering between yaml and Java
 * defaults.
 */
@Import(TestcontainersConfiguration.class)
@SpringBootTest
class BatchPropertiesBindingTests {

	@Autowired
	private BatchProperties properties;

	@Test
	void bindsScheduleDefaults() {
		assertThat(properties.schedule().enabled()).isTrue();
		assertThat(properties.schedule().cron()).isEqualTo("0 0 6 ? * MON");
		assertThat(properties.schedule().zone()).isEqualTo("Asia/Ho_Chi_Minh");
		assertThat(properties.schedule().misfirePolicy()).isEqualTo(BatchProperties.MisfirePolicy.FIRE_ONCE_NOW);
	}

	@Test
	void bindsInputContract() {
		assertThat(properties.input().location()).isEqualTo("/data/incoming");
		assertThat(properties.input().expectedHeader()).containsExactly("customer_id", "field_a", "field_b", "amount",
				"event_date");
		assertThat(properties.input().expectedDelimiter()).isEqualTo(",");
		assertThat(properties.input().expectedEncoding()).isEqualTo("UTF-8");
		assertThat(properties.input().minSizeBytes()).isEqualTo(1);
	}

	@Test
	void bindsProcessingAndRetry() {
		assertThat(properties.processing().chunkSize()).isEqualTo(2000);
		assertThat(properties.processing().skipLimit()).isEqualTo(10_000);
		assertThat(properties.processing().executor().corePoolSize()).isEqualTo(8);
		assertThat(properties.processing().executor().maxPoolSize()).isEqualTo(8);
		assertThat(properties.processing().executor().queueCapacity()).isEqualTo(16);
		assertThat(properties.processing().partitioning().mode())
			.isEqualTo(BatchProperties.PartitioningMode.SINGLE_READER);
		assertThat(properties.retry().maxAttempts()).isEqualTo(3);
		assertThat(properties.retry().initialBackoff()).isEqualTo(Duration.ofMillis(500));
		assertThat(properties.retry().multiplier()).isEqualTo(2.0);
		assertThat(properties.retry().maxBackoff()).isEqualTo(Duration.ofSeconds(5));
	}

}
