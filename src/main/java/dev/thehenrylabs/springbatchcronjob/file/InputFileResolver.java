package dev.thehenrylabs.springbatchcronjob.file;

import java.time.LocalDate;

/**
 * Abstraction over the input file storage provider (SPEC 10). The batch core never
 * depends on a concrete storage system — local filesystem, persistent volume, object
 * storage or an enterprise share map onto this interface.
 */
public interface InputFileResolver {

	/**
	 * Resolve the input file for a weekly business date.
	 * @param businessDate the Monday business date
	 * @return resolved input file metadata
	 * @throws dev.thehenrylabs.springbatchcronjob.exception.InvalidInputFileException
	 * when no valid file is available (missing, unreadable, empty, incorrect convention)
	 */
	ResolvedInputFile resolve(LocalDate businessDate);

}
