package pmj.bdp.spark.lab6;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * Load the lecture sales dataset. Accepts either:
 * <ul>
 *   <li>one denormalized CSV with columns item, category, store, state,
 *       customer, gender, amount (header names are matched loosely), or</li>
 *   <li>a star schema: sales/orders + items + stores + customers, joined on
 *       item / store / customer.</li>
 * </ul>
 */
public final class SalesLoader {

    private SalesLoader() {
    }

    public static JavaRDD<Sale> load(JavaSparkContext sc, Path dataDir) throws IOException {
        if (Files.isRegularFile(dataDir)) {
            return loadDenormalized(sc, dataDir);
        }
        if (!Files.isDirectory(dataDir)) {
            throw new IOException("data path not found: " + dataDir);
        }

        List<Path> csvs = listCsv(dataDir);
        if (csvs.isEmpty()) {
            throw new IOException("no CSV files under " + dataDir);
        }

        Path denorm = pick(csvs, "sales", "sale", "factsales", "fact_sales", "dataset");
        if (denorm != null && hasAllBaseColumns(denorm)) {
            System.out.println("loading denormalized sales from " + denorm.getFileName());
            return loadDenormalized(sc, denorm);
        }

        Path sales = pick(csvs, "sales", "orders", "fact", "transactions");
        Path items = pick(csvs, "items", "item", "products", "product");
        Path stores = pick(csvs, "stores", "store", "locations", "location");
        Path customers = pick(csvs, "customers", "customer", "cust");
        if (sales != null && items != null && stores != null && customers != null) {
            System.out.println("loading star schema:");
            System.out.println("  sales     " + sales.getFileName());
            System.out.println("  items     " + items.getFileName());
            System.out.println("  stores    " + stores.getFileName());
            System.out.println("  customers " + customers.getFileName());
            return loadStar(sc, sales, items, stores, customers);
        }

        if (denorm != null) {
            System.out.println("loading CSV as denormalized sales: " + denorm.getFileName());
            return loadDenormalized(sc, denorm);
        }
        if (csvs.size() == 1) {
            return loadDenormalized(sc, csvs.get(0));
        }
        throw new IOException("could not identify sales dataset in " + dataDir
                + " (files: " + csvs + ")");
    }

    static JavaRDD<Sale> loadDenormalized(JavaSparkContext sc, Path file) throws IOException {
        Header hdr = Header.read(file);
        JavaRDD<String> lines = sc.textFile(file.toString());
        if (hdr.hasHeader) {
            String headerLine = hdr.line;
            lines = lines.filter(l -> !l.equals(headerLine));
        }
        Map<String, Integer> cols = hdr.cols;
        int iItem = require(cols, "item", "itemid", "item_id", "product", "productid", "sku");
        int iCat = require(cols, "category", "cat", "item_category", "product_category");
        int iStore = require(cols, "store", "storeid", "store_id", "shop");
        int iState = require(cols, "state", "store_state", "region");
        int iCust = require(cols, "customer", "cust", "custid", "customer_id", "cust_id");
        int iGender = require(cols, "gender", "sex");
        int iAmt = require(cols, "amount", "sales", "price", "amt", "total", "qtyamount");
        return lines.map(line -> {
            String[] f = split(line);
            if (f.length == 0) {
                return null;
            }
            try {
                return new Sale(
                        get(f, iItem), get(f, iCat), get(f, iStore), get(f, iState),
                        get(f, iCust), get(f, iGender), parseAmount(get(f, iAmt)));
            } catch (RuntimeException e) {
                return null;
            }
        }).filter(s -> s != null);
    }

    static JavaRDD<Sale> loadStar(JavaSparkContext sc, Path sales, Path items,
                                  Path stores, Path customers) throws IOException {
        JavaPairRDD<String, String[]> itemDim = dimByKey(sc, items,
                new String[]{"item", "itemid", "item_id", "product", "productid"},
                new String[]{"category", "cat", "item_category", "product_category"});
        JavaPairRDD<String, String[]> storeDim = dimByKey(sc, stores,
                new String[]{"store", "storeid", "store_id", "shop"},
                new String[]{"state", "store_state", "region"});
        JavaPairRDD<String, String[]> custDim = dimByKey(sc, customers,
                new String[]{"customer", "cust", "custid", "customer_id", "cust_id"},
                new String[]{"gender", "sex"});

        Header sh = Header.read(sales);
        Map<String, Integer> cols = sh.cols;
        int iItem = require(cols, "item", "itemid", "item_id", "product", "productid");
        int iStore = require(cols, "store", "storeid", "store_id", "shop");
        int iCust = require(cols, "customer", "cust", "custid", "customer_id", "cust_id");
        int iAmt = require(cols, "amount", "sales", "price", "amt", "total");
        String headerLine = sh.line;
        boolean skipHeader = sh.hasHeader;

        JavaPairRDD<String, Tuple2<String, Tuple2<String, Double>>> facts = sc.textFile(sales.toString())
                .filter(l -> !skipHeader || !l.equals(headerLine))
                .mapToPair(line -> {
                    String[] f = split(line);
                    if (f.length == 0) {
                        return null;
                    }
                    try {
                        String item = get(f, iItem);
                        String store = get(f, iStore);
                        String cust = get(f, iCust);
                        double amt = parseAmount(get(f, iAmt));
                        return new Tuple2<>(item, new Tuple2<>(store, new Tuple2<>(cust, amt)));
                    } catch (RuntimeException e) {
                        return null;
                    }
                })
                .filter(t -> t != null);

        return facts.join(itemDim)
                .mapToPair(t -> {
                    String item = t._1;
                    String store = t._2._1._1;
                    String cust = t._2._1._2._1;
                    double amt = t._2._1._2._2;
                    String category = t._2._2[0];
                    return new Tuple2<>(store,
                            new Tuple2<>(item, new Tuple2<>(category, new Tuple2<>(cust, amt))));
                })
                .join(storeDim)
                .mapToPair(t -> {
                    String store = t._1;
                    String item = t._2._1._1;
                    String category = t._2._1._2._1;
                    String cust = t._2._1._2._2._1;
                    double amt = t._2._1._2._2._2;
                    String state = t._2._2[0];
                    return new Tuple2<>(cust, new Tuple2<>(
                            new String[]{item, category, store, state}, amt));
                })
                .join(custDim)
                .map(t -> {
                    String customer = t._1;
                    String[] d = t._2._1._1;
                    double amt = t._2._1._2;
                    String gender = t._2._2[0];
                    return new Sale(d[0], d[1], d[2], d[3], customer, gender, amt);
                });
    }

    private static JavaPairRDD<String, String[]> dimByKey(JavaSparkContext sc, Path file,
                                                          String[] keyAliases, String[] valueAliases)
            throws IOException {
        Header hdr = Header.read(file);
        int k = require(hdr.cols, keyAliases);
        int v = require(hdr.cols, valueAliases);
        String headerLine = hdr.line;
        boolean skipHeader = hdr.hasHeader;
        return sc.textFile(file.toString())
                .filter(l -> !skipHeader || !l.equals(headerLine))
                .mapToPair(line -> {
                    String[] f = split(line);
                    if (f.length == 0) {
                        return null;
                    }
                    return new Tuple2<>(get(f, k), new String[]{get(f, v)});
                })
                .filter(t -> t != null)
                .reduceByKey((a, b) -> a);
    }

    private static List<Path> listCsv(Path dir) throws IOException {
        List<Path> out = new ArrayList<>();
        try (Stream<Path> walk = Files.list(dir)) {
            walk.filter(p -> {
                String n = p.getFileName().toString().toLowerCase(Locale.ROOT);
                return Files.isRegularFile(p) && (n.endsWith(".csv") || n.endsWith(".txt"));
            }).forEach(out::add);
        }
        return out;
    }

    private static Path pick(List<Path> files, String... stems) {
        for (String stem : stems) {
            for (Path p : files) {
                String n = p.getFileName().toString().toLowerCase(Locale.ROOT)
                        .replace(".csv", "").replace(".txt", "")
                        .replace("-", "").replace("_", "");
                String s = stem.replace("-", "").replace("_", "");
                if (n.equals(s) || n.startsWith(s)) {
                    return p;
                }
            }
        }
        return null;
    }

    private static boolean hasAllBaseColumns(Path file) throws IOException {
        Header hdr = Header.read(file);
        try {
            require(hdr.cols, "item", "itemid", "item_id", "product", "productid", "sku");
            require(hdr.cols, "category", "cat", "item_category", "product_category");
            require(hdr.cols, "store", "storeid", "store_id", "shop");
            require(hdr.cols, "state", "store_state", "region");
            require(hdr.cols, "customer", "cust", "custid", "customer_id", "cust_id");
            require(hdr.cols, "gender", "sex");
            require(hdr.cols, "amount", "sales", "price", "amt", "total");
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    static final class Header {
        final String line;
        final boolean hasHeader;
        final Map<String, Integer> cols;

        Header(String line, boolean hasHeader, Map<String, Integer> cols) {
            this.line = line;
            this.hasHeader = hasHeader;
            this.cols = cols;
        }

        static Header read(Path file) throws IOException {
            String first;
            try (Stream<String> lines = Files.lines(file)) {
                first = lines.filter(l -> l != null && !l.isBlank()).findFirst().orElse("");
            }
            first = first.replace("\uFEFF", "").trim();
            String[] f = split(first);
            Map<String, Integer> cols = new HashMap<>();
            boolean header = looksLikeHeader(f);
            if (header) {
                for (int i = 0; i < f.length; i++) {
                    cols.put(norm(f[i]), i);
                }
            } else {
                for (int i = 0; i < f.length; i++) {
                    cols.put(String.valueOf(i), i);
                }
            }
            return new Header(first, header, cols);
        }
    }

    private static boolean looksLikeHeader(String[] f) {
        if (f.length == 0) {
            return false;
        }
        int letters = 0;
        for (String s : f) {
            if (s.chars().anyMatch(Character::isLetter)) {
                letters++;
            }
        }
        return letters >= Math.max(2, f.length / 2);
    }

    private static int require(Map<String, Integer> cols, String... aliases) {
        for (String a : aliases) {
            Integer i = cols.get(norm(a));
            if (i != null) {
                return i;
            }
        }
        // positional fallback when there is no header: try well-known 7-col layout
        if (cols.containsKey("0") && cols.size() >= 7) {
            Map<String, Integer> pos = Map.of(
                    "item", 0, "category", 1, "store", 2, "state", 3,
                    "customer", 4, "gender", 5, "amount", 6);
            for (String a : aliases) {
                Integer i = pos.get(norm(a));
                if (i != null && i < cols.size()) {
                    return i;
                }
            }
        }
        throw new IllegalArgumentException("missing column, tried " + String.join(",", aliases)
                + " in " + cols.keySet());
    }

    private static String get(String[] f, int i) {
        if (i < 0 || i >= f.length) {
            throw new IllegalArgumentException("col " + i);
        }
        return f[i].trim();
    }

    static String[] split(String line) {
        if (line == null) {
            return new String[0];
        }
        String raw = line.replace("\uFEFF", "").trim();
        if (raw.isEmpty()) {
            return new String[0];
        }
        String delim = raw.contains("\t") && !raw.contains(",") ? "\t" : ",";
        return raw.split(delim, -1);
    }

    static String norm(String s) {
        return s.toLowerCase(Locale.ROOT).replace(" ", "").replace("-", "").replace("_", "");
    }

    static double parseAmount(String s) {
        String t = s.trim().replace("$", "").replace(",", "");
        return Double.parseDouble(t);
    }
}
