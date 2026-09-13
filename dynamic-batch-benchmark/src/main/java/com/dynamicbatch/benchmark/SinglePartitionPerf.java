package com.dynamicbatch.benchmark;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.dynamicbatch.core.BatchProcessor;
import com.dynamicbatch.core.BatchWorkerGroup;
import com.dynamicbatch.core.pojo.SpoolConfigPOJO;
import com.dynamicbatch.spool.Serializer;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

/**
 * 单分区矩阵压测：扫消息大小 ×（极限吞吐 / 延迟分位），多轮取中位，写出 summary.md + chart.html。
 * <p>直接跑本类 main（IDEA / 干净 JVM）。勿用 mvn exec:java。
 * <p>minTp：ping-pong / 单条在途——发出一条等 flush 完成再发下一条，量端到端延迟。
 */
public class SinglePartitionPerf {

    static {
        // 早于任何 Chronicle Queue 构建；避免 analytics 外连超时刷屏
        System.setProperty("chronicle.announcer.disable", "true");
        System.setProperty("chronicle.analytics.disable", "true");
    }

    private static final int ROUNDS = 5;
    private static final int HEADER = 8;
    private static final int QUEUE_CAPACITY = 4096;
    private static final int BATCH_SIZE = 1000;
    private static final int WARMUP = 20_000;
    private static final String GROUP = "bench";

    private static final int[] SIZES = {128, 256, 4096, 65536};
    private static final int[] QPS_RECORDS = {200_000, 200_000, 50_000, 5_000};
    /** 略增条数，保证 tp99.9 尾部有样本 */
    private static final int[] TP_RECORDS = {20_000, 20_000, 10_000, 5_000};

    private static final double[] TP_PCTS = {0.50, 0.90, 0.95, 0.99, 0.999};
    private static final String[] TP_NAMES = {"tp50", "tp90", "tp95", "tp99", "tp99.9"};
    private static final String[] TP_COLORS = {
            "#2c5f8a", "#1f6f5b", "#c45c26", "#6b4c9a", "#8b3a3a"
    };

    /** 报告页脚口径（md / html 共用） */
    private static final String REPORT_FOOTER_MD =
            "maxQps 看吞吐；minTp 看 ping-pong / 单条在途延迟。图例可点。\n"
                    + "口径：单分区、字节透传（无序列化）、空回调；测的是攒批链路上限，不含业务序列化与业务处理。";
    private static final String REPORT_FOOTER_HTML =
            "maxQps 看吞吐；minTp 看 ping-pong / 单条在途延迟。图例可点。<br/>"
                    + "口径：单分区、字节透传（无序列化）、空回调；测的是攒批链路上限，不含业务序列化与业务处理。";

    private static final AtomicBoolean CLEANED = new AtomicBoolean(false);

    private static final Serializer<byte[]> PASSTHROUGH = new Serializer<byte[]>() {
        @Override
        public byte[] serialize(byte[] data) {
            return data;
        }

        @Override
        public byte[] deserialize(byte[] bytes, Class<byte[]> type) {
            return bytes;
        }
    };

    public static void main(String[] args) throws Exception {
        quietLogs();
        for (int i = 0; i < SIZES.length; i++) {
            requireDivisible(QPS_RECORDS[i], BATCH_SIZE);
            requireDivisible(WARMUP, BATCH_SIZE);
            if (SIZES[i] <= HEADER) {
                throw new IllegalStateException("size must be > " + HEADER);
            }
        }

        Path outDir = resolveOutDir();
        Files.createDirectories(outDir);
        Path spoolRoot = Files.createTempDirectory("single-part-perf-");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> cleanup(spoolRoot), "bench-cleanup"));

        String javaVer = System.getProperty("java.version");
        int cores = Runtime.getRuntime().availableProcessors();
        String ram = detectPhysicalMemory();
        String os = System.getProperty("os.name") + " " + System.getProperty("os.arch");

        System.out.println("=== SinglePartitionPerf 矩阵压测（一键全跑）===");
        System.out.printf("java=%s os=%s cores=%d ram=%s rounds=%d sizes=%s%n",
                javaVer, os, cores, ram, ROUNDS, Arrays.toString(SIZES));
        System.out.println("out=" + outDir.toAbsolutePath());
        System.out.println("延迟分位: " + Arrays.toString(TP_NAMES));

        List<Result> all = new ArrayList<Result>();
        try {
            runOnce(spoolRoot.resolve("proc-warm"), 256, WARMUP, BATCH_SIZE, 10L, false);
            System.gc();

            for (int i = 0; i < SIZES.length; i++) {
                int size = SIZES[i];
                System.out.println();
                System.out.println("==== size=" + size + "B ====");
                all.add(bench("maxQps", spoolRoot, size, QPS_RECORDS[i], BATCH_SIZE, 10L, false));
                all.add(bench("minTp", spoolRoot, size, TP_RECORDS[i], 1, 0L, true));
            }

            printTable(all, javaVer, os, cores, ram);
            writeReports(outDir, javaVer, os, cores, ram, all);
            System.out.println();
            System.out.println("报告已写出: " + outDir.toAbsolutePath());
        } finally {
            cleanup(spoolRoot);
        }
    }

    private static Result bench(String scene, Path root, int msgSize, int records,
                                int batchSize, long maxWaitMs, boolean pingPong) throws Exception {
        String tag = scene + "-" + msgSize;
        runOnce(root.resolve(tag + "-warm"), msgSize, Math.min(WARMUP, records),
                batchSize, maxWaitMs, pingPong);
        System.gc();

        double[] qps = new double[ROUNDS];
        double[][] tpRounds = new double[TP_PCTS.length][ROUNDS];
        for (int r = 0; r < ROUNDS; r++) {
            Round once = runOnce(root.resolve(tag + "-r" + r), msgSize, records,
                    batchSize, maxWaitMs, pingPong);
            qps[r] = once.qps;
            if (pingPong) {
                for (int p = 0; p < TP_PCTS.length; p++) {
                    tpRounds[p][r] = once.tpUs[p];
                }
                System.out.printf(Locale.ROOT,
                        "  %s %dB round%d: %.0f msg/s"
                                + "  tp50=%.3f tp90=%.3f tp95=%.3f tp99=%.3f tp99.9=%.3f µs%n",
                        scene, msgSize, r + 1, once.qps,
                        once.tpUs[0], once.tpUs[1], once.tpUs[2], once.tpUs[3], once.tpUs[4]);
            } else {
                System.out.printf(Locale.ROOT, "  %s %dB round%d: %.0f msg/s%n",
                        scene, msgSize, r + 1, once.qps);
            }
            System.gc();
        }
        double[] tpMedian = new double[TP_PCTS.length];
        if (pingPong) {
            for (int p = 0; p < TP_PCTS.length; p++) {
                tpMedian[p] = median(tpRounds[p]);
            }
        }
        return new Result(scene, msgSize, records, median(qps), tpMedian);
    }

    private static Round runOnce(Path dir, int msgSize, int records, int batchSize,
                                 long maxWaitMs, boolean pingPong) throws Exception {
        Files.createDirectories(dir);
        BatchProcessor processor = new BatchProcessor();
        // minTp 才采延迟：逐条 ns，不做 Histogram 分桶
        final long[] samples = pingPong ? new long[records] : null;
        final AtomicInteger sampleIdx = new AtomicInteger();
        AtomicLong flushed = new AtomicLong();
        int payload = msgSize - HEADER;

        BatchWorkerGroup<byte[]> group = BatchWorkerGroup.builder(byte[].class,
                        SpoolConfigPOJO.builder(dir.toAbsolutePath().toString())
                                .serializer(PASSTHROUGH)
                                .offerTimeoutMs(0L)
                                .build(),
                        batch -> {
                            long now = System.nanoTime();
                            for (byte[] msg : batch) {
                                long latNs = now - ByteBuffer.wrap(msg).getLong();
                                if (samples != null) {
                                    int i = sampleIdx.getAndIncrement();
                                    if (i < samples.length) {
                                        samples[i] = latNs;
                                    }
                                }
                            }
                            flushed.addAndGet(batch.size());
                        })
                .partitionCount(1)
                .queueCapacity(QUEUE_CAPACITY)
                .batchSize(batchSize)
                .maxWaitMs(maxWaitMs)
                .build();
        processor.registerGroup(GROUP, group);

        try {
            long start = System.nanoTime();
            if (pingPong) {
                pumpPingPong(processor, records, payload, flushed);
            } else {
                pumpUnlimited(processor, records, payload);
            }
            awaitFlushed(flushed, records);
            long elapsed = System.nanoTime() - start;
            double[] tpUs = pingPong
                    ? percentilesUs(samples, sampleIdx.get())
                    : new double[TP_PCTS.length];
            return new Round(records * 1e9 / elapsed, tpUs);
        } finally {
            processor.shutdown();
            // 每轮立刻清 spool，避免 ROUNDS 加大把磁盘打满（原先要等整场测完才删）
            deleteQuietly(dir);
        }
    }

    /** 排序 + 线性插值分位，返回 µs（保留亚微秒小数）。 */
    private static double[] percentilesUs(long[] samples, int n) {
        if (n <= 0) {
            throw new IllegalStateException("无延迟样本");
        }
        if (n > samples.length) {
            n = samples.length;
        }
        long[] sorted = Arrays.copyOf(samples, n);
        Arrays.sort(sorted);
        double[] out = new double[TP_PCTS.length];
        for (int i = 0; i < TP_PCTS.length; i++) {
            out[i] = percentileNs(sorted, TP_PCTS[i]) / 1000.0;
        }
        return out;
    }

    private static double percentileNs(long[] sorted, double p) {
        int n = sorted.length;
        if (n == 1) {
            return sorted[0];
        }
        double rank = p * (n - 1);
        int lo = (int) Math.floor(rank);
        int hi = (int) Math.ceil(rank);
        if (lo < 0) {
            lo = 0;
        }
        if (hi >= n) {
            hi = n - 1;
        }
        if (lo == hi) {
            return sorted[lo];
        }
        double w = rank - lo;
        return sorted[lo] * (1.0 - w) + sorted[hi] * w;
    }

    private static void printTable(List<Result> all, String javaVer, String os, int cores, String ram) {
        System.out.println();
        System.out.println(buildMarkdown(all, javaVer, os, cores, ram));
    }

    private static double mbps(Result r) {
        return r.medianQps * r.msgSize / (1024.0 * 1024.0);
    }

    /** 物理内存，如 {@code 16G}；读不到则 {@code ?G}。 */
    private static String detectPhysicalMemory() {
        try {
            java.lang.management.OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();
            if (bean instanceof com.sun.management.OperatingSystemMXBean) {
                long bytes = ((com.sun.management.OperatingSystemMXBean) bean).getTotalPhysicalMemorySize();
                long gib = Math.round(bytes / (1024.0 * 1024.0 * 1024.0));
                if (gib > 0) {
                    return gib + "G";
                }
            }
        } catch (Throwable ignored) {
            // ignore
        }
        return "?G";
    }

    private static List<Result> byScene(List<Result> all, String scene) {
        List<Result> out = new ArrayList<Result>();
        for (Result r : all) {
            if (scene.equals(r.scene)) {
                out.add(r);
            }
        }
        return out;
    }

    private static void writeReports(Path outDir, String javaVer, String os, int cores, String ram,
                                     List<Result> all) throws IOException {
        // 把 Chart.js 拷到结果目录，图例可点且不依赖外网 CDN
        Path chartJsSrc = resolveResourcesRoot().resolve("js/chart.umd.min.js");
        if (Files.isRegularFile(chartJsSrc)) {
            Files.copy(chartJsSrc, outDir.resolve("chart.umd.min.js"), StandardCopyOption.REPLACE_EXISTING);
        }
        Files.write(outDir.resolve("summary.md"),
                buildMarkdown(all, javaVer, os, cores, ram).getBytes(StandardCharsets.UTF_8));
        Files.write(outDir.resolve("chart.html"),
                buildHtml(all, javaVer, os, cores, ram).getBytes(StandardCharsets.UTF_8));
    }

    private static String buildMarkdown(List<Result> all, String javaVer, String os, int cores, String ram) {
        List<Result> qps = byScene(all, "maxQps");
        List<Result> tp = byScene(all, "minTp");
        double baseQps = qps.isEmpty() ? 1 : qps.get(0).medianQps;
        double baseMbps = qps.isEmpty() ? 1 : mbps(qps.get(0));
        double baseTp50 = tp.isEmpty() || tp.get(0).tpUs[0] <= 0 ? 1 : tp.get(0).tpUs[0];

        StringBuilder w = new StringBuilder();
        w.append("# SinglePartitionPerf — 消息大小对比\n\n");
        w.append("- java: `").append(javaVer).append("`\n");
        w.append("- os: `").append(os).append("`\n");
        w.append("- hardware: **").append(cores).append(" 核 / ").append(ram).append("**\n");
        w.append("- rounds: ").append(ROUNDS).append("（中位）\n");
        w.append("- sizes: ").append(Arrays.toString(SIZES)).append("\n");
        w.append("- 相对列：以最小档 **").append(SIZES[0]).append("B** 为 1.00x\n\n");

        w.append("## 1. QPS（maxQps，条/s）\n\n");
        w.append("| size | msg/s | 相对 ").append(SIZES[0]).append("B |\n");
        w.append("|---:|---:|---:|\n");
        for (Result r : qps) {
            w.append(String.format(Locale.ROOT, "| %dB | %,.0f | %.2fx |%n",
                    r.msgSize, r.medianQps, r.medianQps / baseQps));
        }

        w.append("\n## 2. 字节吞吐（maxQps，MB/s）\n\n");
        w.append("| size | MB/s | 相对 ").append(SIZES[0]).append("B |\n");
        w.append("|---:|---:|---:|\n");
        for (Result r : qps) {
            double m = mbps(r);
            w.append(String.format(Locale.ROOT, "| %dB | %.0f | %.2fx |%n",
                    r.msgSize, m, m / baseMbps));
        }

        w.append("\n## 3. 延迟 TP（minTp，µs）\n\n");
        w.append("ping-pong / 单条在途。点击 HTML 图例可勾选分位。\n\n");
        w.append("| size | tp50 | tp90 | tp95 | tp99 | tp99.9 | 相对 ")
                .append(SIZES[0]).append("B tp50 |\n");
        w.append("|---:|---:|---:|---:|---:|---:|---:|\n");
        for (Result r : tp) {
            w.append(String.format(Locale.ROOT,
                    "| %dB | %.3f | %.3f | %.3f | %.3f | %.3f | %.2fx |%n",
                    r.msgSize, r.tpUs[0], r.tpUs[1], r.tpUs[2], r.tpUs[3], r.tpUs[4],
                    r.tpUs[0] / baseTp50));
        }

        w.append("\n---\n");
        w.append(REPORT_FOOTER_MD).append("\n");
        return w.toString();
    }

    private static String buildHtml(List<Result> all, String javaVer, String os, int cores, String ram) {
        List<Result> qps = byScene(all, "maxQps");
        List<Result> tp = byScene(all, "minTp");

        StringBuilder labels = new StringBuilder();
        StringBuilder qpsData = new StringBuilder();
        StringBuilder mbpsData = new StringBuilder();
        for (int i = 0; i < qps.size(); i++) {
            Result r = qps.get(i);
            if (i > 0) {
                labels.append(',');
                qpsData.append(',');
                mbpsData.append(',');
            }
            labels.append('"').append(r.msgSize).append("B\"");
            qpsData.append(String.format(Locale.ROOT, "%.0f", r.medianQps));
            mbpsData.append(String.format(Locale.ROOT, "%.1f", mbps(r)));
        }

        StringBuilder tpLabels = new StringBuilder();
        for (int i = 0; i < tp.size(); i++) {
            if (i > 0) {
                tpLabels.append(',');
            }
            tpLabels.append('"').append(tp.get(i).msgSize).append("B\"");
        }

        StringBuilder tpDatasets = new StringBuilder();
        for (int p = 0; p < TP_PCTS.length; p++) {
            if (p > 0) {
                tpDatasets.append(',');
            }
            StringBuilder data = new StringBuilder();
            for (int i = 0; i < tp.size(); i++) {
                if (i > 0) {
                    data.append(',');
                }
                data.append(String.format(Locale.ROOT, "%.3f", tp.get(i).tpUs[p]));
            }
            tpDatasets.append("{label:'").append(TP_NAMES[p])
                    .append(" µs',data:[").append(data)
                    .append("],backgroundColor:'").append(TP_COLORS[p])
                    .append("',borderRadius:4,maxBarThickness:40}");
        }

        double baseQps = qps.isEmpty() ? 1 : qps.get(0).medianQps;
        double baseMbps = qps.isEmpty() ? 1 : mbps(qps.get(0));
        double baseTp50 = tp.isEmpty() || tp.get(0).tpUs[0] <= 0 ? 1 : tp.get(0).tpUs[0];

        StringBuilder qpsRows = new StringBuilder();
        for (Result r : qps) {
            qpsRows.append(String.format(Locale.ROOT,
                    "<tr><td>%dB</td><td>%s</td><td>%.2fx</td></tr>",
                    r.msgSize, fmtComma(r.medianQps), r.medianQps / baseQps));
        }
        StringBuilder mbpsRows = new StringBuilder();
        for (Result r : qps) {
            double m = mbps(r);
            mbpsRows.append(String.format(Locale.ROOT,
                    "<tr><td>%dB</td><td>%.0f</td><td>%.2fx</td></tr>",
                    r.msgSize, m, m / baseMbps));
        }
        StringBuilder tpRows = new StringBuilder();
        for (Result r : tp) {
            tpRows.append(String.format(Locale.ROOT,
                    "<tr><td>%dB</td><td>%.3f</td><td>%.3f</td><td>%.3f</td><td>%.3f</td><td>%.3f</td><td>%.2fx</td></tr>",
                    r.msgSize, r.tpUs[0], r.tpUs[1], r.tpUs[2], r.tpUs[3], r.tpUs[4],
                    r.tpUs[0] / baseTp50));
        }

        String hw = cores + " 核 / " + ram;

        return "<!DOCTYPE html>\n"
                + "<html lang=\"zh-CN\"><head><meta charset=\"utf-8\"/>"
                + "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"/>"
                + "<title>SinglePartitionPerf — 消息大小对比</title>\n"
                + "<script src=\"chart.umd.min.js\"></script>\n"
                + "<style>\n"
                + ":root{--bg:#f7f5f0;--card:#fff;--ink:#1a1a1a;--muted:#5c5c5c;--line:#e4e0d8;}\n"
                + "*{box-sizing:border-box}\n"
                + "body{margin:0;font:15px/1.5 'Segoe UI',system-ui,sans-serif;color:var(--ink);background:var(--bg)}\n"
                + "header{padding:28px 32px 8px;max-width:1100px;margin:0 auto}\n"
                + "h1{margin:0 0 8px;font-size:1.55rem;letter-spacing:-0.02em}\n"
                + ".meta{color:var(--muted);font-size:0.9rem}\n"
                + "main{max-width:1100px;margin:0 auto;padding:8px 32px 48px;display:grid;gap:20px}\n"
                + "section{background:var(--card);border:1px solid var(--line);border-radius:10px;"
                + "padding:20px 22px 16px}\n"
                + "section h2{margin:0 0 4px;font-size:1.1rem}\n"
                + "section p.hint{margin:0 0 14px;color:var(--muted);font-size:0.88rem}\n"
                + ".chart-box{position:relative;height:300px}\n"
                + "table{width:100%;border-collapse:collapse;margin-top:12px;font-size:0.92rem}\n"
                + "th,td{padding:8px 10px;border-bottom:1px solid var(--line);text-align:right}\n"
                + "th:first-child,td:first-child{text-align:left}\n"
                + "th{color:var(--muted);font-weight:600;font-size:0.78rem;text-transform:uppercase;"
                + "letter-spacing:0.04em}\n"
                + "footer{max-width:1100px;margin:0 auto;padding:0 32px 40px;color:var(--muted);font-size:0.85rem}\n"
                + "</style></head><body>\n"
                + "<header><h1>SinglePartitionPerf — 消息大小对比</h1>"
                + "<div class=\"meta\">java " + esc(javaVer) + " · " + esc(os)
                + " · <b>" + esc(hw) + "</b>"
                + " · " + ROUNDS + " rounds median"
                + " · 相对 " + SIZES[0] + "B=1.00x</div></header>\n"
                + "<main>\n"
                + "<section><h2>1. QPS（条/s）</h2>"
                + "<p class=\"hint\">maxQps：极限灌入吞吐。</p>"
                + "<div class=\"chart-box\"><canvas id=\"cQps\"></canvas></div>"
                + "<table><thead><tr><th>size</th><th>msg/s</th><th>相对 " + SIZES[0]
                + "B</th></tr></thead><tbody>" + qpsRows + "</tbody></table></section>\n"
                + "<section><h2>2. 字节吞吐（MB/s）</h2>"
                + "<p class=\"hint\">同一 maxQps 场景的有效带宽。</p>"
                + "<div class=\"chart-box\"><canvas id=\"cMbps\"></canvas></div>"
                + "<table><thead><tr><th>size</th><th>MB/s</th><th>相对 " + SIZES[0]
                + "B</th></tr></thead><tbody>" + mbpsRows + "</tbody></table></section>\n"
                + "<section><h2>3. 延迟 TP（µs）</h2>"
                + "<p class=\"hint\">minTp：ping-pong / 单条在途。点击图例可勾选/隐藏 tp50·tp90·tp95·tp99·tp99.9。</p>"
                + "<div class=\"chart-box\"><canvas id=\"cTp\"></canvas></div>"
                + "<table><thead><tr><th>size</th><th>tp50</th><th>tp90</th><th>tp95</th>"
                + "<th>tp99</th><th>tp99.9</th><th>相对 " + SIZES[0]
                + "B tp50</th></tr></thead><tbody>" + tpRows + "</tbody></table></section>\n"
                + "</main>\n"
                + "<footer>" + REPORT_FOOTER_HTML + "</footer>\n"
                + "<script>\n"
                + "const labels=[" + labels + "];\n"
                + "const tpLabels=[" + tpLabels + "];\n"
                + "const tip={callbacks:{label:c=>c.dataset.label+': '+Number(c.raw).toLocaleString(undefined,{maximumFractionDigits:3})}};\n"
                + "const base={responsive:true,maintainAspectRatio:false,"
                + "plugins:{legend:{display:false},tooltip:tip},"
                + "scales:{x:{grid:{display:false},ticks:{font:{size:13}}},"
                + "y:{beginAtZero:true,grid:{color:'#eee'},ticks:{font:{size:12}}}}};\n"
                + "new Chart(document.getElementById('cQps'),{type:'bar',data:{labels,"
                + "datasets:[{label:'msg/s',data:[" + qpsData + "],"
                + "backgroundColor:'#1f6f5b',borderRadius:6,maxBarThickness:64}]},options:base});\n"
                + "new Chart(document.getElementById('cMbps'),{type:'bar',data:{labels,"
                + "datasets:[{label:'MB/s',data:[" + mbpsData + "],"
                + "backgroundColor:'#c45c26',borderRadius:6,maxBarThickness:64}]},options:base});\n"
                + "new Chart(document.getElementById('cTp'),{type:'bar',data:{labels:tpLabels,"
                + "datasets:[" + tpDatasets + "]},"
                + "options:Object.assign({},base,{plugins:{legend:{position:'top'},tooltip:tip}})});\n"
                + "</script></body></html>\n";
    }

    private static String esc(String s) {
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }

    private static String fmtComma(double v) {
        return String.format(Locale.US, "%,.0f", v);
    }

    private static Path resolveResourcesRoot() {
        Path cwd = Paths.get("").toAbsolutePath().normalize();
        if (Files.exists(cwd.resolve("dynamic-batch-benchmark/pom.xml"))) {
            return cwd.resolve("dynamic-batch-benchmark/resources");
        }
        if (Files.exists(cwd.resolve("pom.xml"))
                && "dynamic-batch-benchmark".equals(cwd.getFileName().toString())) {
            return cwd.resolve("resources");
        }
        return cwd.resolve("dynamic-batch-benchmark/resources");
    }

    private static Path resolveOutDir() {
        String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
        return resolveResourcesRoot().resolve("bench-results").resolve(ts);
    }

    private static long pumpUnlimited(BatchProcessor processor, int total, int payloadBytes) {
        long rejected = 0;
        for (int i = 0; i < total; i++) {
            byte[] msg = newMsg(payloadBytes);
            while (!processor.submit(GROUP, "k-0", msg)) {
                rejected++;
                if ((rejected & 127) == 0) {
                    LockSupport.parkNanos(1_000_000);
                } else {
                    Thread.yield();
                }
            }
        }
        return rejected;
    }

    private static void pumpPingPong(BatchProcessor processor, int total, int payloadBytes,
                                     AtomicLong flushed) {
        for (int i = 0; i < total; i++) {
            long target = flushed.get() + 1;
            byte[] msg = newMsg(payloadBytes);
            while (!processor.submit(GROUP, "k-0", msg)) {
                Thread.yield();
            }
            while (flushed.get() < target) {
                Thread.yield();
            }
        }
    }

    private static byte[] newMsg(int payloadBytes) {
        ByteBuffer buf = ByteBuffer.allocate(HEADER + payloadBytes);
        buf.putLong(System.nanoTime());
        return buf.array();
    }

    private static void awaitFlushed(AtomicLong flushed, long target) throws InterruptedException {
        long deadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(10);
        while (flushed.get() < target) {
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("flush 未收敛: " + flushed.get() + " < " + target);
            }
            TimeUnit.MILLISECONDS.sleep(20);
        }
    }

    private static double median(double[] a) {
        double[] c = a.clone();
        Arrays.sort(c);
        return c[c.length / 2];
    }

    private static void requireDivisible(int n, int d) {
        if (n % d != 0) {
            throw new IllegalArgumentException(n + " 必须能被 " + d + " 整除");
        }
    }

    private static void quietLogs() {
        try {
            org.slf4j.Logger root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
            if (root instanceof Logger) {
                ((Logger) root).setLevel(Level.WARN);
            }
            ((Logger) LoggerFactory.getLogger(BatchWorkerGroup.class)).setLevel(Level.ERROR);
        } catch (Throwable ignored) {
            // skip
        }
    }

    private static void cleanup(Path dir) {
        if (dir == null || !CLEANED.compareAndSet(false, true)) {
            return;
        }
        deleteQuietly(dir);
    }

    /** 释放 Chronicle 本地资源后尽量删目录（失败不抛，避免干扰压测主流程）。 */
    private static void deleteQuietly(Path dir) {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        try {
            BackgroundResourceReleaser.releasePendingResources();
        } catch (Throwable ignored) {
            // ignore
        }
        System.gc();
        for (int i = 0; i < 3; i++) {
            if (tryDelete(dir)) {
                return;
            }
            try {
                TimeUnit.MILLISECONDS.sleep(300);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private static boolean tryDelete(Path dir) {
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (Exception ignored) {
                    // retry
                }
            });
        } catch (Exception e) {
            return false;
        }
        return !Files.exists(dir);
    }

    private static final class Round {
        final double qps;
        final double[] tpUs;

        Round(double qps, double[] tpUs) {
            this.qps = qps;
            this.tpUs = tpUs;
        }
    }

    private static final class Result {
        final String scene;
        final int msgSize;
        final int records;
        final double medianQps;
        /** 与 TP_PCTS 对齐的中位延迟（µs），仅 minTp 有意义 */
        final double[] tpUs;

        Result(String scene, int msgSize, int records, double medianQps, double[] tpUs) {
            this.scene = scene;
            this.msgSize = msgSize;
            this.records = records;
            this.medianQps = medianQps;
            this.tpUs = tpUs;
        }
    }
}
