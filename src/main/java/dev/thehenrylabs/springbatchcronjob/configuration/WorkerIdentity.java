package dev.thehenrylabs.springbatchcronjob.configuration;

import java.util.UUID;

import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/**
 * Uses Kubernetes' immutable pod UID when present, with a process-local development
 * fallback.
 */
@Component
public class WorkerIdentity {

	private final String workerId;

	private final String podName;

	private final String namespace;

	public WorkerIdentity(Environment environment) {
		this.workerId = value(environment, "POD_UID", UUID.randomUUID().toString());
		this.podName = value(environment, "POD_NAME", "local-" + this.workerId.substring(0, 8));
		this.namespace = value(environment, "POD_NAMESPACE", "default");
	}

	public String workerId() {
		return this.workerId;
	}

	public String podName() {
		return this.podName;
	}

	public String namespace() {
		return this.namespace;
	}

	private static String value(Environment environment, String key, String fallback) {
		String value = environment.getProperty(key);
		return value == null || value.isBlank() ? fallback : value;
	}

}
