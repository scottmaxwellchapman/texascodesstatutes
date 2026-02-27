package group.chapmanlaw.texascodesstatutes;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

public class TexasCodesStatutesSync {

    private static final URI DEFAULT_METADATA_URI = URI.create("https://statutes.capitol.texas.gov/assets/StatuteCodeDownloads.json");
    private static final List<String> DEFAULT_SOURCE_BASE_CANDIDATES = List.of(
            "https://tcss.legis.texas.gov/resources",
            "https://statutes.capitol.texas.gov/resources"
    );
    private static final String DEFAULT_API_BASE = "https://tcss.legis.texas.gov/api/";
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(120);
    private static final Duration POLITE_DOWNLOAD_DELAY = Duration.ofSeconds(3);
    private static final String MARKER_FILE_NAME = ".download_complete.json";
    private static final String LOG_DIRECTORY_NAME = "logs";
    private static final String LOG_FILE_NAME = "texascodesstatutes.log";
    private static final String DEFAULT_CONFIG_BASENAME = "texascodesstatutes_config";
    private static final long LOG_ROTATION_BYTES = 10L * 1024L * 1024L;
    private static final int LOG_ROTATION_FILES = 5;
    private static final PrintStream ORIGINAL_STDOUT = System.out;
    private static final PrintStream ORIGINAL_STDERR = System.err;
    private static volatile boolean rotatingLogInitialized = false;

    public static void main(String[] args) {
        int exitCode = run(args);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    public static int run(String[] args) {
        return run(args, null);
    }

    public static int run(String[] args, Path configPath) {
        initializeRotatingLog();
        try {
            executeSync(args, configPath);
            return 0;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            System.err.println("Interrupted: " + ex.getMessage());
            return 1;
        } catch (Exception ex) {
            System.err.println("Error: " + ex.getMessage());
            return 1;
        }
    }

    public static void executeSync(String[] args) throws IOException, InterruptedException {
        executeSync(args, null);
    }

    public static void executeSync(String[] args, Path configPath) throws IOException, InterruptedException {
        Config config = parseArgs(args, configPath);
        executeSync(config);
    }

    public static void executeSync(Config config) throws IOException, InterruptedException {
        if (config.help()) {
            printUsage();
            return;
        }

        if (config.recurringInterval() != null) {
            runRecurringSync(config);
            return;
        }

        executeSingleSync(config);
    }

    private static void runRecurringSync(Config config) throws InterruptedException {
        Duration interval = config.recurringInterval();
        long cycle = 0;
        while (true) {
            cycle++;
            Instant cycleStartedAt = Instant.now();
            System.out.printf("Starting scheduled sync cycle #%d (%s interval).%n", cycle, interval);
            try {
                executeSingleSync(config);
            } catch (Exception ex) {
                System.err.println("Scheduled sync cycle #" + cycle + " failed: " + ex.getMessage());
            }

            long elapsedMillis = Duration.between(cycleStartedAt, Instant.now()).toMillis();
            long sleepMillis = Math.max(1L, interval.toMillis() - elapsedMillis);
            System.out.printf("Scheduled sync cycle #%d complete. Sleeping for %d ms.%n", cycle, sleepMillis);
            Thread.sleep(sleepMillis);
        }
    }

    private static void executeSingleSync(Config config) throws IOException, InterruptedException {

        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(CONNECT_TIMEOUT)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();

        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        WebhookNotifier webhookNotifier = new WebhookNotifier(httpClient, mapper, config.webhookUrl());

        List<CodeDownload> downloads = fetchCodeDownloads(httpClient, mapper, config.metadataUri());
        if (downloads.isEmpty()) {
            throw new IOException("No downloadable statute codes were discovered.");
        }

        String sourceBase = resolveSourceBase(config, httpClient, downloads);
        try (StorageTarget target = createStorageTarget(config, httpClient)) {
            target.ensureDirectory("");
            SyncSummary summary = syncAll(config, httpClient, mapper, target, sourceBase, downloads, webhookNotifier);
            printSummary(summary, target, sourceBase);
            if (summary.failed() > 0) {
                throw new IOException("Sync completed with " + summary.failed() + " failed code(s).");
            }
        }
    }

    private static void initializeRotatingLog() {
        if (rotatingLogInitialized) {
            return;
        }
        synchronized (TexasCodesStatutesSync.class) {
            if (rotatingLogInitialized) {
                return;
            }
            try {
                Path logPath = Path.of(LOG_DIRECTORY_NAME, LOG_FILE_NAME).toAbsolutePath().normalize();
                RotatingFileOutputStream rotatingOut = new RotatingFileOutputStream(logPath, LOG_ROTATION_BYTES, LOG_ROTATION_FILES);
                System.setOut(new PrintStream(new TeeOutputStream(ORIGINAL_STDOUT, rotatingOut), true, StandardCharsets.UTF_8));
                System.setErr(new PrintStream(new TeeOutputStream(ORIGINAL_STDERR, rotatingOut), true, StandardCharsets.UTF_8));
                rotatingLogInitialized = true;
                System.out.println("Logging to " + logPath + " (rotation 10MB)");
            } catch (Exception ex) {
                ORIGINAL_STDERR.println("Warning: failed to initialize rotating log: " + ex.getMessage());
            }
        }
    }

    private static SyncSummary syncAll(
            Config config,
            HttpClient httpClient,
            ObjectMapper mapper,
            StorageTarget target,
            String sourceBase,
            List<CodeDownload> downloads,
            WebhookNotifier webhookNotifier
    ) throws IOException, InterruptedException {
        int downloaded = 0;
        int skipped = 0;
        int dryRun = 0;
        int failed = 0;
        List<String> failures = new ArrayList<>();
        Instant lastDownloadStartedAt = null;

        int idx = 0;
        for (CodeDownload code : downloads) {
            idx++;
            String setDir = buildSetDirectoryName(code);
            String markerPath = joinRelativePath(setDir, MARKER_FILE_NAME);
            URI zipUri = resolveZipUri(sourceBase, code.wordPath());

            System.out.printf("[%d/%d] %s (%s)%n", idx, downloads.size(), code.codeName(), code.code());

            ProbeResult probe = probeRemoteZip(httpClient, zipUri);
            Optional<CodeMarker> markerOpt = readMarker(target, mapper, markerPath);

            boolean markerUsable = markerOpt
                    .map(m -> m.files != null
                            && !m.files.isEmpty()
                            && m.complete
                            && (m.corruptEntries == null || m.corruptEntries.isEmpty()))
                    .orElse(false);
            boolean markerMatches = markerUsable
                    && probeMatchesMarker(markerOpt.orElseThrow(), probe, zipUri.toString());
            boolean sampleExists = markerMatches && target.exists(markerOpt.orElseThrow().files.get(0));
            boolean shouldDownload = config.force() || !(markerMatches && sampleExists);

            if (!shouldDownload) {
                skipped++;
                System.out.println("  skipped (no change detected and local marker verified)");
                continue;
            }

            if (config.dryRun()) {
                dryRun++;
                System.out.println("  dry-run (would download and extract)");
                continue;
            }

            try {
                target.ensureDirectory(setDir);
                Map<String, String> titleLookup = fetchChapterTitleLookup(httpClient, mapper, code);
                lastDownloadStartedAt = enforcePoliteDelay(lastDownloadStartedAt);
                ExtractionResult extraction = downloadAndExtract(
                        httpClient,
                        target,
                        zipUri,
                        setDir,
                        code,
                        titleLookup,
                        webhookNotifier
                );
                boolean hasCorruptEntries = !extraction.corruptEntries().isEmpty();

                if (hasCorruptEntries && !config.allowPartial()) {
                    throw new IOException(
                            "source ZIP contains corrupt chapter file(s): "
                            + summarizeEntries(extraction.corruptEntries(), 8)
                            + " (run with --allow-partial to keep remaining files)"
                    );
                }

                if (markerOpt.isPresent()) {
                    removeStaleFiles(target, markerOpt.get().files, extraction.files());
                }

                CodeMarker newMarker = buildMarker(
                        zipUri,
                        probe,
                        extraction.files(),
                        extraction.corruptEntries(),
                        !hasCorruptEntries
                );
                target.writeBytes(
                        markerPath,
                        mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(newMarker)
                );
                webhookNotifier.onCodeCompleted(code, markerPath, extraction.files().size(), hasCorruptEntries);

                downloaded++;
                if (hasCorruptEntries) {
                    System.out.printf(
                            "  synced %d files into %s (partial, corrupt: %s)%n",
                            extraction.files().size(),
                            setDir,
                            summarizeEntries(extraction.corruptEntries(), 4)
                    );
                } else {
                    System.out.printf("  synced %d files into %s%n", extraction.files().size(), setDir);
                }
            } catch (Exception ex) {
                failed++;
                String detail = code.codeName() + " (" + code.code() + "): " + ex.getMessage();
                failures.add(detail);
                System.err.println("  failed: " + ex.getMessage());
            }
        }

        return new SyncSummary(downloads.size(), downloaded, skipped, dryRun, failed, failures);
    }

    private static Optional<CodeMarker> readMarker(StorageTarget target, ObjectMapper mapper, String markerPath) throws IOException {
        Optional<byte[]> raw = target.readIfExists(markerPath);
        if (raw.isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(mapper.readValue(raw.get(), CodeMarker.class));
        } catch (IOException ex) {
            System.err.println("  marker parse failed at " + markerPath + ", forcing refresh: " + ex.getMessage());
            return Optional.empty();
        }
    }

    private static void removeStaleFiles(StorageTarget target, List<String> previousFiles, List<String> currentFiles) throws IOException {
        if (previousFiles == null || previousFiles.isEmpty()) {
            return;
        }
        Set<String> keep = new HashSet<>();
        for (String current : currentFiles) {
            keep.add(normalizeRelativePath(current));
        }
        for (String previous : previousFiles) {
            if (isBlank(previous)) {
                continue;
            }
            String normalized;
            try {
                normalized = normalizeRelativePath(previous);
            } catch (IOException ex) {
                continue;
            }
            if (!keep.contains(normalized)) {
                target.deleteIfExists(normalized);
            }
        }
    }

    private static boolean probeMatchesMarker(CodeMarker marker, ProbeResult probe, String expectedSourceUrl) {
        if (marker == null) {
            return false;
        }
        if (!marker.complete || (marker.corruptEntries != null && !marker.corruptEntries.isEmpty())) {
            return false;
        }
        if (probe.statusCode() < 200 || probe.statusCode() >= 400) {
            return false;
        }
        if (!Objects.equals(trimToNull(marker.sourceUrl), trimToNull(expectedSourceUrl))) {
            return false;
        }

        boolean sawSignal = false;

        if (!isBlank(probe.etag())) {
            sawSignal = true;
            if (!Objects.equals(trimToNull(marker.etag), trimToNull(probe.etag()))) {
                return false;
            }
        }

        if (!isBlank(probe.lastModified())) {
            sawSignal = true;
            if (!Objects.equals(trimToNull(marker.lastModified), trimToNull(probe.lastModified()))) {
                return false;
            }
        }

        if (probe.contentLength() >= 0) {
            sawSignal = true;
            if (marker.contentLength < 0 || marker.contentLength != probe.contentLength()) {
                return false;
            }
        }

        return sawSignal;
    }

    private static Instant enforcePoliteDelay(Instant lastDownloadStartedAt) throws InterruptedException {
        if (lastDownloadStartedAt == null) {
            return Instant.now();
        }
        long elapsedMillis = Duration.between(lastDownloadStartedAt, Instant.now()).toMillis();
        long delayMillis = POLITE_DOWNLOAD_DELAY.toMillis() - elapsedMillis;
        if (delayMillis > 0) {
            Thread.sleep(delayMillis);
        }
        return Instant.now();
    }

    private static Map<String, String> fetchChapterTitleLookup(
            HttpClient httpClient,
            ObjectMapper mapper,
            CodeDownload code
    ) {
        Map<String, String> lookup = new LinkedHashMap<>();
        String codePrefix = code.code() == null ? "" : code.code().toLowerCase(Locale.ROOT);
        if (isBlank(code.codeId()) || isBlank(codePrefix)) {
            return lookup;
        }

        List<String> chapterTypes = chapterTypesForCode(code.code());
        for (String chapterType : chapterTypes) {
            try {
                URI uri = buildApiUri("QuickSearch", "PopulateChapterList", code.codeId(), chapterType);
                List<QuickSearchEntry> entries = fetchQuickSearchEntries(httpClient, mapper, uri);
                if (entries.isEmpty()) {
                    continue;
                }
                for (QuickSearchEntry entry : entries) {
                    addTitleLookupEntry(lookup, codePrefix, entry.url, entry.text);
                }
                if (!lookup.isEmpty()) {
                    break;
                }
            } catch (Exception ex) {
                // Continue without title enrichment if endpoint fails for this type.
            }
        }

        if ("cv".equals(codePrefix)) {
            try {
                URI artUri = buildApiUri("QuickSearch", "PopulateArtSecList", "CV", code.codeId(), "AS", "0");
                List<QuickSearchEntry> artEntries = fetchQuickSearchEntries(httpClient, mapper, artUri);
                for (QuickSearchEntry entry : artEntries) {
                    addTitleLookupEntry(lookup, codePrefix, entry.url, entry.text);
                    addTitleLookupEntry(lookup, codePrefix, "CV." + entry.url, entry.text);
                    addTitleLookupEntry(lookup, codePrefix, "CV." + entry.url + ".0", entry.text);
                }
            } catch (Exception ex) {
                // Continue with whichever title lookups were already collected.
            }
        }

        return lookup;
    }

    private static List<String> chapterTypesForCode(String code) {
        String normalized = code == null ? "" : code.trim().toUpperCase(Locale.ROOT);
        if ("CN".equals(normalized)) {
            return List.of("AR", "CH");
        }
        if ("BA".equals(normalized)) {
            return List.of("PT", "CH");
        }
        return List.of("CH", "AR", "PT");
    }

    private static URI buildApiUri(String... pathSegments) {
        StringBuilder builder = new StringBuilder(DEFAULT_API_BASE);
        if (!DEFAULT_API_BASE.endsWith("/")) {
            builder.append('/');
        }
        for (int i = 0; i < pathSegments.length; i++) {
            if (i > 0) {
                builder.append('/');
            }
            builder.append(encodePathSegment(pathSegments[i]));
        }
        return URI.create(builder.toString());
    }

    private static String encodePathSegment(String input) {
        String safe = input == null ? "" : input;
        return URLEncoder.encode(safe, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private static List<QuickSearchEntry> fetchQuickSearchEntries(
            HttpClient httpClient,
            ObjectMapper mapper,
            URI uri
    ) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(uri)
                .GET()
                .timeout(REQUEST_TIMEOUT)
                .build();
        HttpResponse<InputStream> response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            return List.of();
        }
        try (InputStream body = response.body()) {
            QuickSearchEntry[] entries = mapper.readValue(body, QuickSearchEntry[].class);
            if (entries == null || entries.length == 0) {
                return List.of();
            }
            return List.of(entries);
        }
    }

    private static void addTitleLookupEntry(
            Map<String, String> lookup,
            String codePrefix,
            String rawKey,
            String rawTitle
    ) {
        String title = normalizeTitle(rawTitle);
        String key = normalizeLookupKey(rawKey);
        if (isBlank(title) || isBlank(key)) {
            return;
        }

        putIfAbsentNonBlank(lookup, key, title);
        putIfAbsentNonBlank(lookup, stripVersionSuffix(key), title);

        String prefixed = codePrefix + "." + key;
        putIfAbsentNonBlank(lookup, prefixed, title);
        putIfAbsentNonBlank(lookup, stripVersionSuffix(prefixed), title);

        if (key.endsWith(".0")) {
            putIfAbsentNonBlank(lookup, key.substring(0, key.length() - 2), title);
            if (prefixed.endsWith(".0")) {
                putIfAbsentNonBlank(lookup, prefixed.substring(0, prefixed.length() - 2), title);
            }
        }

        if (key.startsWith(codePrefix + ".")) {
            String naked = key.substring(codePrefix.length() + 1);
            putIfAbsentNonBlank(lookup, naked, title);
            putIfAbsentNonBlank(lookup, stripVersionSuffix(naked), title);
            if (naked.endsWith(".0")) {
                putIfAbsentNonBlank(lookup, naked.substring(0, naked.length() - 2), title);
            }
        }
    }

    private static void putIfAbsentNonBlank(Map<String, String> lookup, String key, String value) {
        if (!isBlank(key) && !isBlank(value)) {
            lookup.putIfAbsent(key, value);
        }
    }

    private static String normalizeLookupKey(String rawKey) {
        if (rawKey == null) {
            return "";
        }
        String key = rawKey.trim().replace('\\', '/');
        int lastSlash = key.lastIndexOf('/');
        if (lastSlash >= 0) {
            key = key.substring(lastSlash + 1);
        }
        String lower = key.toLowerCase(Locale.ROOT);
        if (lower.endsWith(".docx") || lower.endsWith(".htm") || lower.endsWith(".pdf")) {
            lower = lower.substring(0, lower.lastIndexOf('.'));
        }
        return stripVersionSuffix(lower);
    }

    private static String stripVersionSuffix(String key) {
        if (key == null) {
            return "";
        }
        return key.replaceFirst("(?i)\\.v\\d+$", "");
    }

    private static String normalizeTitle(String rawTitle) {
        if (rawTitle == null) {
            return "";
        }
        return rawTitle.trim().replaceAll("\\s+", " ");
    }

    private static String buildOutputRelativePath(
            String setDir,
            String safeEntryName,
            CodeDownload code,
            Map<String, String> titleLookup
    ) throws IOException {
        int slash = safeEntryName.lastIndexOf('/');
        String relDir = slash >= 0 ? safeEntryName.substring(0, slash) : "";
        String fileName = slash >= 0 ? safeEntryName.substring(slash + 1) : safeEntryName;

        int dot = fileName.lastIndexOf('.');
        if (dot <= 0) {
            return joinRelativePath(setDir, safeEntryName);
        }

        String stem = fileName.substring(0, dot);
        String extension = fileName.substring(dot);
        String enrichedName = fileName;
        String title = findChapterTitleForStem(stem, code.code(), titleLookup);
        if (!isBlank(title)) {
            String readable = sanitizeTitleForFilename(title);
            if (!isBlank(readable)) {
                enrichedName = stem + " - " + readable + extension;
            }
        }

        String relative = relDir.isEmpty() ? enrichedName : relDir + "/" + enrichedName;
        return joinRelativePath(setDir, relative);
    }

    private static String findChapterTitleForStem(String stem, String code, Map<String, String> titleLookup) {
        if (titleLookup == null || titleLookup.isEmpty() || isBlank(stem) || isBlank(code)) {
            return null;
        }

        String codePrefix = code.toLowerCase(Locale.ROOT);
        String normalizedStem = normalizeLookupKey(stem);
        if (isBlank(normalizedStem)) {
            return null;
        }

        Set<String> candidates = new LinkedHashSet<>();
        candidates.add(normalizedStem);
        if (normalizedStem.endsWith(".0")) {
            candidates.add(normalizedStem.substring(0, normalizedStem.length() - 2));
        }

        if (normalizedStem.startsWith(codePrefix + ".")) {
            String naked = normalizedStem.substring(codePrefix.length() + 1);
            candidates.add(naked);
            if (naked.endsWith(".0")) {
                candidates.add(naked.substring(0, naked.length() - 2));
            }
        } else {
            candidates.add(codePrefix + "." + normalizedStem);
            if (normalizedStem.endsWith(".0")) {
                candidates.add(codePrefix + "." + normalizedStem.substring(0, normalizedStem.length() - 2));
            }
        }

        for (String candidate : candidates) {
            String title = titleLookup.get(candidate);
            if (!isBlank(title)) {
                return title;
            }
        }
        return null;
    }

    private static String sanitizeTitleForFilename(String title) {
        String cleaned = normalizeTitle(title);
        if (cleaned.isEmpty()) {
            return cleaned;
        }

        cleaned = cleaned.replaceFirst("(?i)^(chapter|article|part)\\s+[^.]+\\.\\s*", "");
        cleaned = cleaned.replace("&", "and");
        cleaned = cleaned.replaceAll("[\\\\/:*?\"<>|]", " ");
        cleaned = cleaned.replaceAll("\\s+", " ").trim();
        if (cleaned.length() > 120) {
            cleaned = cleaned.substring(0, 120).trim();
        }
        return cleaned;
    }

    private static ExtractionResult downloadAndExtract(
            HttpClient httpClient,
            StorageTarget target,
            URI zipUri,
            String setDir,
            CodeDownload code,
            Map<String, String> titleLookup,
            WebhookNotifier webhookNotifier
    )
            throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(zipUri)
                .GET()
                .timeout(REQUEST_TIMEOUT)
                .build();

        HttpResponse<InputStream> response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("Download failed [" + response.statusCode() + "] for " + zipUri);
        }
        long expectedLength = parseLong(firstHeader(response.headers(), "Content-Length"), -1);
        Path tempZip = Files.createTempFile("texas-codes-", ".zip");
        Set<String> writtenFiles = new TreeSet<>();
        Set<String> corruptEntries = new TreeSet<>();

        try {
            try (InputStream body = response.body()) {
                Files.copy(body, tempZip, StandardCopyOption.REPLACE_EXISTING);
            }
            if (expectedLength >= 0 && Files.size(tempZip) != expectedLength) {
                throw new IOException(
                        "Downloaded ZIP size mismatch for " + zipUri
                        + " (expected " + expectedLength + ", got " + Files.size(tempZip) + ")"
                );
            }

            try (ZipFile zip = new ZipFile(tempZip.toFile())) {
                Enumeration<? extends ZipEntry> entries = zip.entries();
                while (entries.hasMoreElements()) {
                    ZipEntry entry = entries.nextElement();
                    if (entry.isDirectory()) {
                        continue;
                    }

                    String safeEntryName = sanitizeZipEntryName(entry.getName());
                    if (safeEntryName == null) {
                        continue;
                    }

                    String outputRelativePath = buildOutputRelativePath(setDir, safeEntryName, code, titleLookup);
                    try (InputStream entryStream = zip.getInputStream(entry)) {
                        byte[] bytes = entryStream.readAllBytes();
                        target.writeBytes(outputRelativePath, bytes);
                        writtenFiles.add(outputRelativePath);
                        webhookNotifier.onFileDownloaded(code, outputRelativePath);
                    } catch (IOException ex) {
                        if (isZipCorruption(ex)) {
                            corruptEntries.add(safeEntryName);
                            continue;
                        }
                        throw ex;
                    }
                }
            }

            if (writtenFiles.isEmpty()) {
                throw new IOException("ZIP contained no readable files: " + zipUri);
            }

            return new ExtractionResult(List.copyOf(writtenFiles), List.copyOf(corruptEntries));
        } finally {
            Files.deleteIfExists(tempZip);
        }
    }

    private static CodeMarker buildMarker(
            URI source,
            ProbeResult probe,
            List<String> files,
            List<String> corruptEntries,
            boolean complete
    ) {
        CodeMarker marker = new CodeMarker();
        marker.sourceUrl = source.toString();
        marker.etag = probe.etag();
        marker.lastModified = probe.lastModified();
        marker.contentLength = probe.contentLength();
        marker.downloadedAtUtc = Instant.now().toString();
        marker.files = new ArrayList<>(files);
        marker.corruptEntries = corruptEntries == null ? new ArrayList<>() : new ArrayList<>(corruptEntries);
        marker.complete = complete && marker.corruptEntries.isEmpty();
        return marker;
    }

    private static ProbeResult probeRemoteZip(HttpClient httpClient, URI uri) {
        try {
            HttpRequest request = HttpRequest.newBuilder(uri)
                    .timeout(REQUEST_TIMEOUT)
                    .method("HEAD", HttpRequest.BodyPublishers.noBody())
                    .build();
            HttpResponse<Void> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
            HttpHeaders headers = response.headers();
            return new ProbeResult(
                    response.statusCode(),
                    firstHeader(headers, "ETag"),
                    firstHeader(headers, "Last-Modified"),
                    parseLong(firstHeader(headers, "Content-Length"), -1),
                    firstHeader(headers, "Content-Type"),
                    null
            );
        } catch (Exception ex) {
            return new ProbeResult(-1, null, null, -1, null, ex.getMessage());
        }
    }

    private static List<CodeDownload> fetchCodeDownloads(HttpClient httpClient, ObjectMapper mapper, URI metadataUri)
            throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(metadataUri)
                .GET()
                .timeout(REQUEST_TIMEOUT)
                .build();
        HttpResponse<InputStream> response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("Failed to load metadata [" + response.statusCode() + "] from " + metadataUri);
        }
        StatuteCodeDownloadsPayload payload;
        try (InputStream body = response.body()) {
            payload = mapper.readValue(body, StatuteCodeDownloadsPayload.class);
        }
        List<CodeDownload> result = new ArrayList<>();
        if (payload == null || payload.statuteCode == null) {
            return result;
        }
        for (StatuteCodeDownloadEntry entry : payload.statuteCode) {
            if (entry == null) {
                continue;
            }
            String code = trimToNull(entry.code);
            String codeName = trimToNull(entry.codeName);
            String word = trimToNull(entry.word);
            if (isBlank(code) || isBlank(codeName) || isBlank(word)) {
                continue;
            }
            result.add(new CodeDownload(trimToNull(entry.codeId), code, codeName, word));
        }
        return result;
    }

    private static String resolveSourceBase(Config config, HttpClient httpClient, List<CodeDownload> downloads) {
        if (!isBlank(config.sourceBaseOverride())) {
            return stripTrailingSlash(config.sourceBaseOverride());
        }

        String sampleWordPath = downloads.stream()
                .map(CodeDownload::wordPath)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse("/Zips/AG.doc.zip");

        for (String candidate : DEFAULT_SOURCE_BASE_CANDIDATES) {
            URI sampleZip = resolveZipUri(candidate, sampleWordPath);
            ProbeResult probe = probeRemoteZip(httpClient, sampleZip);
            if (probe.statusCode() >= 200 && probe.statusCode() < 300 && looksLikeZip(probe, sampleWordPath)) {
                return stripTrailingSlash(candidate);
            }
        }

        return stripTrailingSlash(DEFAULT_SOURCE_BASE_CANDIDATES.get(0));
    }

    private static boolean looksLikeZip(ProbeResult probe, String sampleWordPath) {
        if (!isBlank(probe.contentType()) && probe.contentType().toLowerCase(Locale.ROOT).contains("zip")) {
            return true;
        }
        return sampleWordPath.toLowerCase(Locale.ROOT).endsWith(".zip") && probe.contentLength() > 0;
    }

    private static StorageTarget createStorageTarget(Config config, HttpClient httpClient) throws Exception {
        return switch (config.targetType()) {
            case LOCAL -> new LocalStorageTarget(config.dataDir());
            case SFTP -> new SftpStorageTarget(config.sftp());
            case WEBDAV -> new WebDavStorageTarget(httpClient, config.webDav());
        };
    }

    private static Config parseArgs(String[] args, Path defaultConfigPath) {
        Map<String, String> cliOptions = new LinkedHashMap<>();
        Set<String> cliFlags = new LinkedHashSet<>();

        for (String arg : args) {
            if (!arg.startsWith("--")) {
                throw new IllegalArgumentException("Invalid argument: " + arg);
            }

            String body = arg.substring(2);
            int eqIdx = body.indexOf('=');
            if (eqIdx >= 0) {
                String key = body.substring(0, eqIdx).trim();
                String value = body.substring(eqIdx + 1).trim();
                if (key.isEmpty()) {
                    throw new IllegalArgumentException("Invalid option format: " + arg);
                }
                cliOptions.put(key, value);
            } else {
                cliFlags.add(body.trim());
            }
        }

        validateKnownArgs(cliOptions.keySet(), cliFlags);

        boolean help = cliFlags.contains("help");
        if (help) {
            return new Config(
                    true,
                    TargetType.LOCAL,
                    Path.of("texascodesstatutes_data"),
                    DEFAULT_METADATA_URI,
                    null,
                    false,
                    false,
                    false,
                    null,
                    null,
                    null,
                    null
            );
        }

        Path resolvedConfigPath = resolveConfigPath(cliOptions.get("config"), defaultConfigPath);
        PropertyOverrides propertyOverrides = loadPropertyOverrides(resolvedConfigPath);
        validateKnownArgs(propertyOverrides.options().keySet(), propertyOverrides.flags());

        Map<String, String> options = new LinkedHashMap<>(propertyOverrides.options());
        options.putAll(cliOptions);
        options.remove("config");

        Set<String> flags = new LinkedHashSet<>(propertyOverrides.flags());
        flags.addAll(cliFlags);

        TargetType targetType = parseTargetType(options.getOrDefault("target", "local"));
        Path dataDir = Path.of(options.getOrDefault("data-dir", "texascodesstatutes_data")).normalize();
        URI metadataUri = URI.create(options.getOrDefault("metadata-url", DEFAULT_METADATA_URI.toString()));
        String sourceBaseOverride = trimToNull(options.get("source-base"));
        Duration recurringInterval = parseOptionalRecurringInterval(options.get("interval"));
        URI webhookUrl = parseOptionalUri(options.get("webhook-url"), "webhook-url");
        boolean force = flags.contains("force");
        boolean dryRun = flags.contains("dry-run");
        boolean allowPartial = flags.contains("allow-partial");

        SftpConfig sftpConfig = null;
        WebDavConfig webDavConfig = null;

        if (targetType == TargetType.SFTP) {
            String host = required(options, "sftp-host");
            int port = parseInt(options.getOrDefault("sftp-port", "22"), "sftp-port");
            String user = required(options, "sftp-user");
            String password = optionOrEnv(options, "sftp-password", "SFTP_PASSWORD");
            String privateKey = trimToNull(options.get("sftp-private-key"));
            String keyPassphrase = optionOrEnv(options, "sftp-private-key-passphrase", "SFTP_PRIVATE_KEY_PASSPHRASE");
            String rootDir = options.getOrDefault("sftp-root", "/");

            Path keyPath = privateKey == null ? null : Path.of(privateKey).toAbsolutePath().normalize();
            if (isBlank(password) && keyPath == null) {
                throw new IllegalArgumentException("SFTP target requires either --sftp-password or --sftp-private-key.");
            }

            sftpConfig = new SftpConfig(host, port, user, password, keyPath, keyPassphrase, rootDir);
        } else if (targetType == TargetType.WEBDAV) {
            URI webDavUrl = URI.create(required(options, "webdav-url"));
            String user = trimToNull(options.get("webdav-user"));
            String password = optionOrEnv(options, "webdav-password", "WEBDAV_PASSWORD");
            if (!isBlank(password) && isBlank(user)) {
                throw new IllegalArgumentException("--webdav-user is required when a WebDAV password is provided.");
            }
            webDavConfig = new WebDavConfig(webDavUrl, user, password);
        }

        return new Config(
                false,
                targetType,
                dataDir,
                metadataUri,
                sourceBaseOverride,
                force,
                dryRun,
                allowPartial,
                recurringInterval,
                webhookUrl,
                sftpConfig,
                webDavConfig
        );
    }

    private static void validateKnownArgs(Set<String> optionKeys, Set<String> flags) {
        Set<String> knownOptions = Set.of(
                "config",
                "target",
                "data-dir",
                "metadata-url",
                "source-base",
                "interval",
                "webhook-url",
                "sftp-host",
                "sftp-port",
                "sftp-user",
                "sftp-password",
                "sftp-private-key",
                "sftp-private-key-passphrase",
                "sftp-root",
                "webdav-url",
                "webdav-user",
                "webdav-password"
        );
        Set<String> knownFlags = Set.of("help", "force", "dry-run", "allow-partial");

        for (String key : optionKeys) {
            if (!knownOptions.contains(key)) {
                throw new IllegalArgumentException("Unknown option: --" + key);
            }
        }
        for (String flag : flags) {
            if (!knownFlags.contains(flag)) {
                throw new IllegalArgumentException("Unknown flag: --" + flag);
            }
        }
    }

    private static TargetType parseTargetType(String raw) {
        String normalized = raw == null ? "local" : raw.trim().toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "local" -> TargetType.LOCAL;
            case "sftp" -> TargetType.SFTP;
            case "webdav" -> TargetType.WEBDAV;
            default -> throw new IllegalArgumentException("Unsupported target type: " + raw);
        };
    }

    private static String required(Map<String, String> options, String key) {
        String value = trimToNull(options.get(key));
        if (value == null) {
            throw new IllegalArgumentException("Missing required option: --" + key);
        }
        return value;
    }

    private static String optionOrEnv(Map<String, String> options, String optionKey, String envKey) {
        String fromOption = trimToNull(options.get(optionKey));
        if (fromOption != null) {
            return fromOption;
        }
        return trimToNull(System.getenv(envKey));
    }

    private static Path resolveConfigPath(String rawConfigPath, Path defaultConfigPath) {
        if (rawConfigPath != null) {
            String cliConfigPath = trimToNull(rawConfigPath);
            if (cliConfigPath == null) {
                throw new IllegalArgumentException("--config requires a non-empty path.");
            }
            return Path.of(cliConfigPath);
        }
        if (defaultConfigPath != null) {
            return defaultConfigPath;
        }

        Path propertiesPath = Path.of(DEFAULT_CONFIG_BASENAME + ".properties");
        if (Files.exists(propertiesPath) && !Files.isDirectory(propertiesPath)) {
            return propertiesPath;
        }
        Path barePath = Path.of(DEFAULT_CONFIG_BASENAME);
        if (Files.exists(barePath) && !Files.isDirectory(barePath)) {
            return barePath;
        }
        return null;
    }

    private static PropertyOverrides loadPropertyOverrides(Path configPath) {
        if (configPath == null) {
            return PropertyOverrides.empty();
        }

        Path path = configPath.toAbsolutePath().normalize();
        if (!Files.exists(path) || Files.isDirectory(path)) {
            throw new IllegalArgumentException("Config file not found: " + path);
        }

        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(path)) {
            properties.load(in);
        } catch (IOException ex) {
            throw new IllegalArgumentException("Unable to read config file " + path + ": " + ex.getMessage(), ex);
        }

        Map<String, String> options = new LinkedHashMap<>();
        Set<String> flags = new LinkedHashSet<>();

        for (String rawKey : properties.stringPropertyNames()) {
            String canonicalKey = canonicalizePropertyKey(rawKey);
            if (canonicalKey == null) {
                throw new IllegalArgumentException("Unknown key in config file " + path + ": " + rawKey);
            }

            String rawValue = properties.getProperty(rawKey);
            if (isFlagKey(canonicalKey)) {
                if (parseBooleanProperty(rawValue, rawKey, path)) {
                    flags.add(canonicalKey);
                }
            } else {
                String value = trimToNull(rawValue);
                if (value != null) {
                    options.put(canonicalKey, value);
                }
            }
        }

        return new PropertyOverrides(options, flags);
    }

    private static String canonicalizePropertyKey(String rawKey) {
        if (rawKey == null) {
            return null;
        }
        String key = rawKey.trim().toLowerCase(Locale.ROOT)
                .replace('.', '-')
                .replace('_', '-');

        return switch (key) {
            case "help",
                    "force",
                    "dry-run",
                    "allow-partial",
                    "target",
                    "data-dir",
                    "metadata-url",
                    "source-base",
                    "webhook-url",
                    "sftp-host",
                    "sftp-port",
                    "sftp-user",
                    "sftp-password",
                    "sftp-private-key",
                    "sftp-private-key-passphrase",
                    "sftp-root",
                    "webdav-url",
                    "webdav-user",
                    "webdav-password" -> key;
            default -> null;
        };
    }

    private static boolean isFlagKey(String key) {
        return "help".equals(key)
                || "force".equals(key)
                || "dry-run".equals(key)
                || "allow-partial".equals(key);
    }

    private static boolean parseBooleanProperty(String rawValue, String propertyKey, Path configPath) {
        String value = trimToNull(rawValue);
        if (value == null) {
            return false;
        }
        String normalized = value.toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "1", "true", "yes", "on" -> true;
            case "0", "false", "no", "off" -> false;
            default -> throw new IllegalArgumentException(
                    "Invalid boolean value for " + propertyKey + " in " + configPath + ": " + rawValue
            );
        };
    }

    private static URI parseOptionalUri(String raw, String optionName) {
        String value = trimToNull(raw);
        if (value == null) {
            return null;
        }
        try {
            return URI.create(value);
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("Invalid URI for --" + optionName + ": " + value);
        }
    }

    private static String buildSetDirectoryName(CodeDownload code) {
        String prefix = code.code().toUpperCase(Locale.ROOT);
        String slug = slugify(code.codeName());
        return prefix + "-" + slug;
    }

    private static String slugify(String value) {
        String normalized = value
                .toLowerCase(Locale.ROOT)
                .replace("&", " and ")
                .replaceAll("[^a-z0-9]+", "-")
                .replaceAll("^-+", "")
                .replaceAll("-+$", "")
                .replaceAll("-{2,}", "-");
        return normalized.isBlank() ? "code" : normalized;
    }

    private static URI resolveZipUri(String sourceBase, String relativeWordPath) {
        String base = stripTrailingSlash(sourceBase);
        String path = relativeWordPath == null ? "" : relativeWordPath.trim();
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        return URI.create(base + path);
    }

    private static String stripTrailingSlash(String input) {
        String value = input == null ? "" : input.trim();
        while (value.endsWith("/") && value.length() > 1) {
            value = value.substring(0, value.length() - 1);
        }
        return value;
    }

    private static String sanitizeZipEntryName(String rawName) throws IOException {
        if (rawName == null) {
            return null;
        }
        String prepared = rawName.trim().replace('\\', '/');
        while (prepared.startsWith("/")) {
            prepared = prepared.substring(1);
        }
        if (prepared.isEmpty()) {
            return null;
        }
        String[] parts = prepared.split("/");
        List<String> safeParts = new ArrayList<>();
        for (String part : parts) {
            if (part == null || part.isBlank() || ".".equals(part)) {
                continue;
            }
            if ("..".equals(part)) {
                throw new IOException("Unsafe ZIP entry path: " + rawName);
            }
            safeParts.add(part);
        }
        if (safeParts.isEmpty()) {
            return null;
        }
        return String.join("/", safeParts);
    }

    private static String normalizeRelativePath(String path) throws IOException {
        if (path == null) {
            return "";
        }
        String prepared = path.trim().replace('\\', '/');
        while (prepared.startsWith("/")) {
            prepared = prepared.substring(1);
        }
        if (prepared.isEmpty()) {
            return "";
        }
        String[] parts = prepared.split("/");
        List<String> safeParts = new ArrayList<>();
        for (String part : parts) {
            if (part == null || part.isBlank() || ".".equals(part)) {
                continue;
            }
            if ("..".equals(part)) {
                throw new IOException("Unsafe relative path: " + path);
            }
            safeParts.add(part);
        }
        return String.join("/", safeParts);
    }

    private static String joinRelativePath(String left, String right) throws IOException {
        if (isBlank(left)) {
            return normalizeRelativePath(right);
        }
        if (isBlank(right)) {
            return normalizeRelativePath(left);
        }
        return normalizeRelativePath(left + "/" + right);
    }

    private static String firstHeader(HttpHeaders headers, String name) {
        return headers.firstValue(name).orElse(null);
    }

    private static long parseLong(String raw, long fallback) {
        if (isBlank(raw)) {
            return fallback;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException ex) {
            return fallback;
        }
    }

    private static int parseInt(String raw, String optionName) {
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid integer for --" + optionName + ": " + raw);
        }
    }

    private static Duration parseOptionalRecurringInterval(String raw) {
        String value = trimToNull(raw);
        if (value == null) {
            return null;
        }

        Duration interval;
        try {
            if (value.matches("^\\d+$")) {
                interval = Duration.ofSeconds(Long.parseLong(value));
            } else {
                interval = Duration.parse(value);
            }
        } catch (Exception ex) {
            throw new IllegalArgumentException(
                    "Invalid --interval value: " + value + " (use seconds like 300 or ISO-8601 like PT5M)"
            );
        }

        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("--interval must be greater than zero.");
        }
        return interval;
    }

    private static boolean isZipCorruption(IOException ex) {
        Throwable cursor = ex;
        while (cursor != null) {
            if (cursor instanceof ZipException) {
                return true;
            }
            cursor = cursor.getCause();
        }
        String msg = ex.getMessage();
        if (msg == null) {
            return false;
        }
        String lower = msg.toLowerCase(Locale.ROOT);
        return lower.contains("invalid compressed")
                || lower.contains("invalid stored block")
                || lower.contains("inflate")
                || lower.contains("unexpected end of zlib")
                || lower.contains("zlib input stream");
    }

    private static String summarizeEntries(List<String> entries, int limit) {
        if (entries == null || entries.isEmpty()) {
            return "";
        }
        if (entries.size() <= limit) {
            return String.join(", ", entries);
        }
        return String.join(", ", entries.subList(0, limit)) + " +" + (entries.size() - limit) + " more";
    }

    private static String fileNameFromRelativePath(String relativePath) {
        if (isBlank(relativePath)) {
            return "";
        }
        String normalized = relativePath.replace('\\', '/');
        int idx = normalized.lastIndexOf('/');
        return idx >= 0 ? normalized.substring(idx + 1) : normalized;
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static void printSummary(SyncSummary summary, StorageTarget target, String sourceBase) {
        System.out.println();
        System.out.println("Sync complete.");
        System.out.println("  Source base: " + sourceBase);
        System.out.println("  Target: " + target.describe());
        System.out.println("  Total sets: " + summary.total());
        System.out.println("  Downloaded: " + summary.downloaded());
        System.out.println("  Skipped: " + summary.skipped());
        System.out.println("  Dry-run: " + summary.dryRun());
        System.out.println("  Failed: " + summary.failed());

        if (!summary.failures().isEmpty()) {
            System.out.println("  Failures:");
            for (String failure : summary.failures()) {
                System.out.println("    - " + failure);
            }
        }
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("  java -jar texascodesstatutes.jar [options]");
        System.out.println();
        System.out.println("Core options:");
        System.out.println("  --target=local|sftp|webdav       Storage target (default: local)");
        System.out.println("  --config=/path/config.properties Load options from a properties file (CLI overrides; default: texascodesstatutes_config[.properties])");
        System.out.println("  --data-dir=PATH                  Local output directory (default: texascodesstatutes_data)");
        System.out.println("  --metadata-url=URL               Metadata JSON URL");
        System.out.println("  --source-base=URL                Override ZIP source base URL");
        System.out.println("  --interval=VALUE                 Recurring timer interval (seconds or ISO-8601, e.g. 300 or PT5M)");
        System.out.println("  --webhook-url=URL                Optional POST webhook for file/completion events");
        System.out.println("  --force                          Force redownload all sets");
        System.out.println("  --dry-run                        Show what would be downloaded");
        System.out.println("  --allow-partial                  Keep good files when a source ZIP has corrupt entries");
        System.out.println("  --help                           Show this help");
        System.out.println("  (Downloads are throttled with a 3-second delay between ZIP fetches.)");
        System.out.println();
        System.out.println("SFTP options:");
        System.out.println("  --sftp-host=HOST");
        System.out.println("  --sftp-port=22");
        System.out.println("  --sftp-user=USER");
        System.out.println("  --sftp-password=PASSWORD         or SFTP_PASSWORD env");
        System.out.println("  --sftp-private-key=/path/key");
        System.out.println("  --sftp-private-key-passphrase=PW or SFTP_PRIVATE_KEY_PASSPHRASE env");
        System.out.println("  --sftp-root=/remote/path");
        System.out.println();
        System.out.println("WebDAV options:");
        System.out.println("  --webdav-url=https://server/path");
        System.out.println("  --webdav-user=USER");
        System.out.println("  --webdav-password=PASSWORD       or WEBDAV_PASSWORD env");
        System.out.println();
        System.out.println("Config file keys (same as flags/options):");
        System.out.println("  webhook.url, sftp.host, sftp.user, sftp.password, webdav.url, webdav.user, webdav.password");
        System.out.println("  (Use dotted, underscored, or dashed forms; example: sftp-private-key-passphrase)");
    }

    private enum TargetType {
        LOCAL,
        SFTP,
        WEBDAV
    }

    private record Config(
            boolean help,
            TargetType targetType,
            Path dataDir,
            URI metadataUri,
            String sourceBaseOverride,
            boolean force,
            boolean dryRun,
            boolean allowPartial,
            Duration recurringInterval,
            URI webhookUrl,
            SftpConfig sftp,
            WebDavConfig webDav
    ) {
    }

    private record SftpConfig(
            String host,
            int port,
            String user,
            String password,
            Path privateKeyPath,
            String privateKeyPassphrase,
            String rootDir
    ) {
    }

    private record WebDavConfig(
            URI baseUri,
            String user,
            String password
    ) {
    }

    private record CodeDownload(String codeId, String code, String codeName, String wordPath) {
    }

    private record ProbeResult(
            int statusCode,
            String etag,
            String lastModified,
            long contentLength,
            String contentType,
            String error
    ) {
    }

    private record ExtractionResult(List<String> files, List<String> corruptEntries) {
    }

    private record SyncSummary(
            int total,
            int downloaded,
            int skipped,
            int dryRun,
            int failed,
            List<String> failures
    ) {
    }

    private record PropertyOverrides(
            Map<String, String> options,
            Set<String> flags
    ) {

        private static PropertyOverrides empty() {
            return new PropertyOverrides(Map.of(), Set.of());
        }
    }

    private static final class TeeOutputStream extends OutputStream {

        private final OutputStream primary;
        private final OutputStream secondary;

        private TeeOutputStream(OutputStream primary, OutputStream secondary) {
            this.primary = Objects.requireNonNull(primary, "primary");
            this.secondary = Objects.requireNonNull(secondary, "secondary");
        }

        @Override
        public synchronized void write(int b) throws IOException {
            primary.write(b);
            secondary.write(b);
        }

        @Override
        public synchronized void write(byte[] b, int off, int len) throws IOException {
            Objects.checkFromIndexSize(off, len, b.length);
            primary.write(b, off, len);
            secondary.write(b, off, len);
        }

        @Override
        public synchronized void flush() throws IOException {
            primary.flush();
            secondary.flush();
        }

        @Override
        public synchronized void close() throws IOException {
            IOException failure = null;
            try {
                primary.flush();
            } catch (IOException ex) {
                failure = ex;
            }
            try {
                secondary.close();
            } catch (IOException ex) {
                if (failure == null) {
                    failure = ex;
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    private static final class RotatingFileOutputStream extends OutputStream {

        private final Path logFile;
        private final long maxBytes;
        private final int maxArchives;
        private OutputStream fileOut;
        private long currentSize;
        private boolean closed;

        private RotatingFileOutputStream(Path logFile, long maxBytes, int maxArchives) throws IOException {
            this.logFile = Objects.requireNonNull(logFile, "logFile");
            this.maxBytes = maxBytes;
            this.maxArchives = Math.max(0, maxArchives);
            Path parent = logFile.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            this.currentSize = Files.exists(logFile) ? Files.size(logFile) : 0L;
            this.fileOut = Files.newOutputStream(
                    logFile,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND,
                    StandardOpenOption.WRITE
            );
        }

        @Override
        public synchronized void write(int b) throws IOException {
            byte[] one = new byte[]{(byte) b};
            write(one, 0, 1);
        }

        @Override
        public synchronized void write(byte[] b, int off, int len) throws IOException {
            Objects.checkFromIndexSize(off, len, b.length);
            ensureOpen();
            if (len == 0) {
                return;
            }
            rotateIfNeeded(len);
            fileOut.write(b, off, len);
            currentSize += len;
        }

        @Override
        public synchronized void flush() throws IOException {
            ensureOpen();
            fileOut.flush();
        }

        @Override
        public synchronized void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            fileOut.close();
        }

        private void ensureOpen() throws IOException {
            if (closed) {
                throw new IOException("RotatingFileOutputStream is closed.");
            }
        }

        private void rotateIfNeeded(int pendingBytes) throws IOException {
            if (maxBytes <= 0) {
                return;
            }
            if (currentSize + pendingBytes <= maxBytes) {
                return;
            }
            rotate();
        }

        private void rotate() throws IOException {
            fileOut.close();

            if (maxArchives > 0) {
                for (int i = maxArchives - 1; i >= 1; i--) {
                    Path from = archivePath(i);
                    Path to = archivePath(i + 1);
                    if (Files.exists(from)) {
                        Files.move(from, to, StandardCopyOption.REPLACE_EXISTING);
                    }
                }
                if (Files.exists(logFile)) {
                    Files.move(logFile, archivePath(1), StandardCopyOption.REPLACE_EXISTING);
                }
            } else {
                Files.deleteIfExists(logFile);
            }

            fileOut = Files.newOutputStream(
                    logFile,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE
            );
            currentSize = 0L;
        }

        private Path archivePath(int idx) {
            return Path.of(logFile.toString() + "." + idx);
        }
    }

    private static final class WebhookNotifier {

        private final HttpClient httpClient;
        private final ObjectMapper mapper;
        private final URI webhookUri;

        private WebhookNotifier(HttpClient httpClient, ObjectMapper mapper, URI webhookUri) {
            this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
            this.mapper = Objects.requireNonNull(mapper, "mapper");
            this.webhookUri = webhookUri;
        }

        private void onFileDownloaded(CodeDownload code, String outputRelativePath) {
            if (webhookUri == null) {
                return;
            }
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("event", "file_downloaded");
            payload.put("code", code.code());
            payload.put("codeName", code.codeName());
            payload.put("statuteName", code.codeName());
            payload.put("newFileName", fileNameFromRelativePath(outputRelativePath));
            payload.put("relativePath", outputRelativePath);
            payload.put("timestampUtc", Instant.now().toString());
            post(payload);
        }

        private void onCodeCompleted(CodeDownload code, String markerPath, int fileCount, boolean partial) {
            if (webhookUri == null) {
                return;
            }
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("event", "code_download_completed");
            payload.put("code", code.code());
            payload.put("codeName", code.codeName());
            payload.put("statuteName", code.codeName());
            payload.put("newFileName", fileNameFromRelativePath(markerPath));
            payload.put("relativePath", markerPath);
            payload.put("fileCount", fileCount);
            payload.put("partial", partial);
            payload.put("timestampUtc", Instant.now().toString());
            post(payload);
        }

        private void post(Map<String, Object> payload) {
            try {
                byte[] body = mapper.writeValueAsBytes(payload);
                HttpRequest request = HttpRequest.newBuilder(webhookUri)
                        .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                        .header("Content-Type", "application/json")
                        .timeout(REQUEST_TIMEOUT)
                        .build();
                HttpResponse<Void> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
                if (response.statusCode() < 200 || response.statusCode() >= 300) {
                    System.err.println("  webhook warning: received HTTP " + response.statusCode() + " from " + webhookUri);
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                System.err.println("  webhook warning: interrupted while posting to " + webhookUri);
            } catch (Exception ex) {
                System.err.println("  webhook warning: " + ex.getMessage());
            }
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class CodeMarker {

        public String sourceUrl;
        public String etag;
        public String lastModified;
        public long contentLength = -1;
        public String downloadedAtUtc;
        public boolean complete = true;
        public List<String> files = new ArrayList<>();
        public List<String> corruptEntries = new ArrayList<>();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class StatuteCodeDownloadsPayload {

        @JsonProperty("StatuteCode")
        public List<StatuteCodeDownloadEntry> statuteCode = new ArrayList<>();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class StatuteCodeDownloadEntry {

        @JsonProperty("codeID")
        public String codeId;
        public String code;
        @JsonProperty("CodeName")
        public String codeName;
        @JsonProperty("Word")
        public String word;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class QuickSearchEntry {

        public String text;
        public String value;
        public String url;
    }

    private interface StorageTarget extends AutoCloseable {

        String describe();

        boolean exists(String relativePath) throws IOException;

        Optional<byte[]> readIfExists(String relativePath) throws IOException;

        void writeBytes(String relativePath, byte[] bytes) throws IOException;

        void deleteIfExists(String relativePath) throws IOException;

        void ensureDirectory(String relativeDir) throws IOException;

        @Override
        default void close() throws Exception {
        }
    }

    private static final class LocalStorageTarget implements StorageTarget {

        private final Path root;

        private LocalStorageTarget(Path root) throws IOException {
            this.root = root.toAbsolutePath().normalize();
            Files.createDirectories(this.root);
        }

        @Override
        public String describe() {
            return "local:" + root;
        }

        @Override
        public boolean exists(String relativePath) throws IOException {
            return Files.exists(resolve(relativePath));
        }

        @Override
        public Optional<byte[]> readIfExists(String relativePath) throws IOException {
            Path path = resolve(relativePath);
            if (!Files.exists(path) || Files.isDirectory(path)) {
                return Optional.empty();
            }
            return Optional.of(Files.readAllBytes(path));
        }

        @Override
        public void writeBytes(String relativePath, byte[] bytes) throws IOException {
            Path path = resolve(relativePath);
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.write(path, bytes);
        }

        @Override
        public void deleteIfExists(String relativePath) throws IOException {
            Files.deleteIfExists(resolve(relativePath));
        }

        @Override
        public void ensureDirectory(String relativeDir) throws IOException {
            Files.createDirectories(resolve(relativeDir));
        }

        private Path resolve(String relativePath) throws IOException {
            String normalized = normalizeRelativePath(relativePath);
            Path resolved = normalized.isEmpty() ? root : root.resolve(normalized).normalize();
            if (!resolved.startsWith(root)) {
                throw new IOException("Path escapes local root: " + relativePath);
            }
            return resolved;
        }
    }

    private static final class SftpStorageTarget implements StorageTarget {

        private final SftpConfig config;
        private final Session session;
        private final ChannelSftp channel;
        private final String rootDir;

        private SftpStorageTarget(SftpConfig config) throws JSchException, SftpException, IOException {
            this.config = Objects.requireNonNull(config, "sftp config");
            this.rootDir = normalizeRootDir(config.rootDir());

            JSch jsch = new JSch();
            if (config.privateKeyPath() != null) {
                if (isBlank(config.privateKeyPassphrase())) {
                    jsch.addIdentity(config.privateKeyPath().toString());
                } else {
                    jsch.addIdentity(
                            config.privateKeyPath().toString(),
                            config.privateKeyPassphrase().getBytes(StandardCharsets.UTF_8)
                    );
                }
            }

            this.session = jsch.getSession(config.user(), config.host(), config.port());
            if (!isBlank(config.password())) {
                this.session.setPassword(config.password());
            }

            Properties sshConfig = new Properties();
            sshConfig.put("StrictHostKeyChecking", "no");
            this.session.setConfig(sshConfig);
            this.session.connect((int) CONNECT_TIMEOUT.toMillis());

            this.channel = (ChannelSftp) this.session.openChannel("sftp");
            this.channel.connect((int) CONNECT_TIMEOUT.toMillis());

            ensureAbsoluteDirectory(this.rootDir);
        }

        @Override
        public String describe() {
            return "sftp://" + config.user() + "@" + config.host() + ":" + config.port() + "/" + rootDir;
        }

        @Override
        public boolean exists(String relativePath) throws IOException {
            String full = resolve(relativePath);
            try {
                channel.lstat(full);
                return true;
            } catch (SftpException ex) {
                if (isSftpNotFound(ex)) {
                    return false;
                }
                throw new IOException("SFTP exists check failed for " + full, ex);
            }
        }

        @Override
        public Optional<byte[]> readIfExists(String relativePath) throws IOException {
            String full = resolve(relativePath);
            try (InputStream in = channel.get(full)) {
                return Optional.of(in.readAllBytes());
            } catch (SftpException ex) {
                if (isSftpNotFound(ex)) {
                    return Optional.empty();
                }
                throw new IOException("SFTP read failed for " + full, ex);
            }
        }

        @Override
        public void writeBytes(String relativePath, byte[] bytes) throws IOException {
            String full = resolve(relativePath);
            ensureAbsoluteDirectory(parentDirectory(full));
            try (ByteArrayInputStream in = new ByteArrayInputStream(bytes)) {
                channel.put(in, full, ChannelSftp.OVERWRITE);
            } catch (SftpException ex) {
                throw new IOException("SFTP write failed for " + full, ex);
            }
        }

        @Override
        public void deleteIfExists(String relativePath) throws IOException {
            String full = resolve(relativePath);
            try {
                channel.rm(full);
            } catch (SftpException ex) {
                if (!isSftpNotFound(ex)) {
                    throw new IOException("SFTP delete failed for " + full, ex);
                }
            }
        }

        @Override
        public void ensureDirectory(String relativeDir) throws IOException {
            String full = resolve(relativeDir);
            ensureAbsoluteDirectory(full);
        }

        @Override
        public void close() {
            if (channel != null && channel.isConnected()) {
                channel.disconnect();
            }
            if (session != null && session.isConnected()) {
                session.disconnect();
            }
        }

        private String resolve(String relativePath) throws IOException {
            String normalized = normalizeRelativePath(relativePath);
            if (normalized.isEmpty()) {
                return rootDir;
            }
            if ("/".equals(rootDir)) {
                return "/" + normalized;
            }
            return rootDir + "/" + normalized;
        }

        private void ensureAbsoluteDirectory(String directory) throws IOException {
            String normalized = directory == null ? "" : directory.replace('\\', '/');
            if (normalized.isBlank() || ".".equals(normalized) || "/".equals(normalized)) {
                return;
            }

            boolean absolute = normalized.startsWith("/");
            String[] parts = normalized.split("/");
            String current = absolute ? "/" : "";

            for (String part : parts) {
                if (part == null || part.isBlank() || ".".equals(part)) {
                    continue;
                }
                if ("..".equals(part)) {
                    throw new IOException("Unsafe SFTP directory path: " + directory);
                }
                if (current.isEmpty() || current.endsWith("/")) {
                    current = current + part;
                } else {
                    current = current + "/" + part;
                }
                mkdirIfMissing(current);
            }
        }

        private void mkdirIfMissing(String fullPath) throws IOException {
            try {
                var attrs = channel.stat(fullPath);
                if (!attrs.isDir()) {
                    throw new IOException("Remote path exists but is not a directory: " + fullPath);
                }
            } catch (SftpException ex) {
                if (isSftpNotFound(ex)) {
                    try {
                        channel.mkdir(fullPath);
                    } catch (SftpException mkEx) {
                        throw new IOException("Unable to create remote directory: " + fullPath, mkEx);
                    }
                } else {
                    throw new IOException("Unable to stat remote directory: " + fullPath, ex);
                }
            }
        }

        private static boolean isSftpNotFound(SftpException ex) {
            return ex.id == ChannelSftp.SSH_FX_NO_SUCH_FILE;
        }

        private static String normalizeRootDir(String root) {
            String normalized = root == null ? "/" : root.trim().replace('\\', '/');
            if (normalized.isEmpty()) {
                normalized = "/";
            }
            while (normalized.endsWith("/") && normalized.length() > 1) {
                normalized = normalized.substring(0, normalized.length() - 1);
            }
            return normalized;
        }
    }

    private static final class WebDavStorageTarget implements StorageTarget {

        private final HttpClient httpClient;
        private final URI baseUri;
        private final String authorizationHeader;

        private WebDavStorageTarget(HttpClient httpClient, WebDavConfig config) {
            this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
            this.baseUri = ensureTrailingSlash(config.baseUri());
            this.authorizationHeader = buildAuthorizationHeader(config.user(), config.password());
        }

        @Override
        public String describe() {
            return "webdav:" + baseUri;
        }

        @Override
        public boolean exists(String relativePath) throws IOException {
            URI uri = buildUri(relativePath);
            HttpResponse<Void> response = sendDiscarding(request(uri).method("HEAD", HttpRequest.BodyPublishers.noBody()));
            int status = response.statusCode();
            if (status >= 200 && status < 300) {
                return true;
            }
            if (status == 404) {
                return false;
            }
            if (status == 405) {
                HttpRequest.Builder propFind = request(uri)
                        .header("Depth", "0")
                        .method("PROPFIND", HttpRequest.BodyPublishers.noBody());
                HttpResponse<Void> propFindResponse = sendDiscarding(propFind);
                if (propFindResponse.statusCode() >= 200 && propFindResponse.statusCode() < 300) {
                    return true;
                }
                if (propFindResponse.statusCode() == 404) {
                    return false;
                }
            }
            throw new IOException("WebDAV exists check failed [" + status + "] for " + uri);
        }

        @Override
        public Optional<byte[]> readIfExists(String relativePath) throws IOException {
            URI uri = buildUri(relativePath);
            HttpResponse<byte[]> response = sendBytes(request(uri).GET());
            if (response.statusCode() == 404) {
                return Optional.empty();
            }
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IOException("WebDAV read failed [" + response.statusCode() + "] for " + uri);
            }
            return Optional.of(response.body());
        }

        @Override
        public void writeBytes(String relativePath, byte[] bytes) throws IOException {
            String normalized = normalizeRelativePath(relativePath);
            ensureDirectory(parentDirectory(normalized));
            URI uri = buildUri(normalized);
            HttpResponse<Void> response = sendDiscarding(request(uri).PUT(HttpRequest.BodyPublishers.ofByteArray(bytes)));
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IOException("WebDAV write failed [" + response.statusCode() + "] for " + uri);
            }
        }

        @Override
        public void deleteIfExists(String relativePath) throws IOException {
            URI uri = buildUri(relativePath);
            HttpResponse<Void> response = sendDiscarding(request(uri).method("DELETE", HttpRequest.BodyPublishers.noBody()));
            int status = response.statusCode();
            if ((status >= 200 && status < 300) || status == 404) {
                return;
            }
            throw new IOException("WebDAV delete failed [" + status + "] for " + uri);
        }

        @Override
        public void ensureDirectory(String relativeDir) throws IOException {
            String normalized = normalizeRelativePath(relativeDir);
            if (normalized.isEmpty()) {
                return;
            }

            String current = "";
            for (String part : normalized.split("/")) {
                if (part.isBlank()) {
                    continue;
                }
                current = current.isEmpty() ? part : current + "/" + part;
                mkcol(current);
            }
        }

        private void mkcol(String relativeDir) throws IOException {
            URI uri = buildUri(relativeDir);
            HttpRequest.Builder builder = request(uri).method("MKCOL", HttpRequest.BodyPublishers.noBody());
            HttpResponse<Void> response = sendDiscarding(builder);
            int status = response.statusCode();
            if (status == 201 || status == 200 || status == 204 || status == 301 || status == 302 || status == 405) {
                return;
            }
            throw new IOException("WebDAV MKCOL failed [" + status + "] for " + uri);
        }

        private URI buildUri(String relativePath) throws IOException {
            String normalized = normalizeRelativePath(relativePath);
            if (normalized.isEmpty()) {
                return baseUri;
            }
            return URI.create(baseUri.toString() + encodeRelativePath(normalized));
        }

        private HttpRequest.Builder request(URI uri) {
            HttpRequest.Builder builder = HttpRequest.newBuilder(uri).timeout(REQUEST_TIMEOUT);
            if (authorizationHeader != null) {
                builder.header("Authorization", authorizationHeader);
            }
            return builder;
        }

        private HttpResponse<Void> sendDiscarding(HttpRequest.Builder builder) throws IOException {
            try {
                return httpClient.send(builder.build(), HttpResponse.BodyHandlers.discarding());
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted during WebDAV request", ex);
            }
        }

        private HttpResponse<byte[]> sendBytes(HttpRequest.Builder builder) throws IOException {
            try {
                return httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted during WebDAV request", ex);
            }
        }

        private static URI ensureTrailingSlash(URI baseUri) {
            String raw = baseUri.toString();
            if (raw.endsWith("/")) {
                return baseUri;
            }
            return URI.create(raw + "/");
        }

        private static String buildAuthorizationHeader(String user, String password) {
            if (isBlank(user)) {
                return null;
            }
            String token = user + ":" + (password == null ? "" : password);
            return "Basic " + Base64.getEncoder().encodeToString(token.getBytes(StandardCharsets.UTF_8));
        }

        private static String encodeRelativePath(String relativePath) {
            String[] parts = relativePath.split("/");
            StringBuilder encoded = new StringBuilder();
            for (int i = 0; i < parts.length; i++) {
                if (i > 0) {
                    encoded.append('/');
                }
                encoded.append(URLEncoder.encode(parts[i], StandardCharsets.UTF_8).replace("+", "%20"));
            }
            return encoded.toString();
        }
    }

    private static String parentDirectory(String relativePath) {
        if (relativePath == null || relativePath.isBlank()) {
            return "";
        }
        int idx = relativePath.lastIndexOf('/');
        if (idx < 0) {
            return "";
        }
        return relativePath.substring(0, idx);
    }
}
