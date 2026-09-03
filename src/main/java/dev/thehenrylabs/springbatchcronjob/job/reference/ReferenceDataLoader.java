package dev.thehenrylabs.springbatchcronjob.job.reference;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import dev.thehenrylabs.springbatchcronjob.exception.BatchInvariantException;
import dev.thehenrylabs.springbatchcronjob.job.processing.ProcessingContext;
import dev.thehenrylabs.springbatchcronjob.service.BatchLaunchService;

/**
 * loadReferenceDataStep (SPEC 20): load the stable lookup set once per run into an
 * immutable in-memory snapshot; no per-record DB/REST calls anywhere in processing.
 */
@Component
public class ReferenceDataLoader implements Tasklet {

	private static final Logger log = LoggerFactory.getLogger(ReferenceDataLoader.class);

	private static final String RESOURCE = "classpath:reference/eligible-customers.csv";

	private final ProcessingContext processingContext;

	public ReferenceDataLoader(ProcessingContext processingContext) {
		this.processingContext = processingContext;
	}

	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
		JobParameters parameters = chunkContext.getStepContext().getStepExecution().getJobParameters();
		LocalDate businessDate = LocalDate.parse(parameters.getString(BatchLaunchService.PARAM_BUSINESS_DATE));
		Set<String> eligible = loadEligible();
		this.processingContext.publish(businessDate, eligible);
		log.info("Reference data loaded businessDate={} eligibleCustomers={}", businessDate, eligible.size());
		return RepeatStatus.FINISHED;
	}

	private Set<String> loadEligible() {
		Set<String> ids = new HashSet<>();
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(
				new ClassPathResource("reference/eligible-customers.csv").getInputStream(), StandardCharsets.UTF_8))) {
			String line;
			while ((line = reader.readLine()) != null) {
				String trimmed = line.trim();
				if (!trimmed.isEmpty()) {
					ids.add(trimmed);
				}
			}
			return ids;
		}
		catch (IOException e) {
			throw new BatchInvariantException("Cannot load reference data " + RESOURCE, e);
		}
	}

}
