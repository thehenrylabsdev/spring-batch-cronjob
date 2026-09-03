package dev.thehenrylabs.springbatchcronjob.file;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.springframework.stereotype.Service;

import dev.thehenrylabs.springbatchcronjob.exception.InvalidInputFileException;

/**
 * Streamed SHA-256 computation (SPEC 11, 19).
 *
 * <p>
 * The digest is computed exactly once per logical file at launch time (the application's
 * ingestion boundary), never again inside the processing job — the manifest row carries
 * the registered checksum and the validate step verifies the input against it.
 */
@Service
public class FileChecksumService {

	private static final int BUFFER_SIZE = 128 * 1024;

	private static final char[] HEX = "0123456789abcdef".toCharArray();

	/** Returns the lowercase hex digest of {@code file}. */
	public String sha256(Path file) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			try (InputStream in = new DigestInputStream(Files.newInputStream(file), digest)) {
				byte[] buffer = new byte[BUFFER_SIZE];
				// The digest update happens inside DigestInputStream.read; the return
				// value is not otherwise needed.
				while (in.read(buffer) >= 0) {
					// fully consumed
				}
			}
			return toHex(digest.digest());
		}
		catch (IOException | NoSuchAlgorithmException e) {
			throw new InvalidInputFileException("Cannot compute SHA-256 for " + file, e);
		}
	}

	private static String toHex(byte[] digest) {
		char[] out = new char[digest.length * 2];
		int i = 0;
		for (byte b : digest) {
			out[i++] = HEX[(b >>> 4) & 0x0F];
			out[i++] = HEX[b & 0x0F];
		}
		return new String(out);
	}

}
