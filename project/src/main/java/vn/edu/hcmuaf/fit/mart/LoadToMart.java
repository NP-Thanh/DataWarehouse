package vn.edu.hcmuaf.fit.mart;

import com.opencsv.CSVWriter;
import vn.edu.hcmuaf.fit.db.DataMartDBConfig;
import vn.edu.hcmuaf.fit.db.WarehouseDBConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.io.FileWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LoadToMart {

    private static final String AGGREGATE_DIR = "data/aggregate";
    private static final String DATE_STR = new SimpleDateFormat("dd_MM_yyyy").format(new Date());

    public static void load() throws Exception {
        LoggerUtil.log("=== SCRIPT 6: LOAD TO DATA MART - HOÀN THIỆN + TOP 10 + TOP 20 ĐẮT NHẤT ===");
        Files.createDirectories(Paths.get(AGGREGATE_DIR));
        LoggerUtil.startProcess(LoggerUtil.SOURCE_CELLPHONES_ID, "Load to Data Mart");

        try {
            // 6.1. Xuất 2 file CSV tổng hợp
            // 6.1.1 aggregate_daily_summary_[dd_MM_yyyy].csv
            // 6.1.2 aggregate_daily_sales_[dd_MM_yyyy].csv
            exportAggregateToCSV();

            // 6.2. Tạo và nạp dữ liệu cho 5 bảng Mart trong schema data_mart
            // 6.2.1 Tạo bảng mart_daily_summary
            createAndLoadMartDailySummary();
            // 6.2.2 Tạo bảng mart_daily_sales
            createAndLoadMartDailySales();
            // 6.2.3 Tạo bảng mart_product_summary → Giá trung bình 30 ngày gần nhất
            createAndLoadMartProductSummary();
            // 6.2.4 Tạo bảng mart_top10_daily_change → Top 10 thay đổi giá mạnh nhất
            createAndLoadTop10DailyChange();
            // 6.2.5 Tạo bảng mart_today_top_price → Top 20 sản phẩm đắt nhất hôm nay
            createAndLoadTodayTopPrice();

            // 6.3. Sao chép 2 bảng chiều từ warehouse_db sang data_mart
            // 6.3.1 dim_date
            // 6.3.2 dim_product
            copyDimDate();
            copyDimProduct();

            LoggerUtil.endProcess(0, "SUCCESS", null);
            LoggerUtil.log("HOÀN TẤT 100%! Đã tạo đủ 2 file CSV + 7 bảng trong data_mart");
            LoggerUtil.log("7 bảng trong data_mart: mart_daily_summary, mart_daily_sales, mart_product_summary, mart_top10_daily_change, mart_today_top_price, dim_date, dim_product");

        } catch (Exception e) {
            LoggerUtil.endProcess(0, "FAILED", e.getMessage());
            throw e;
        }
    }
    // 6.1. Xuất 2 file CSV tổng hợp
    // 6.1.1 aggregate_daily_summary_[dd_MM_yyyy].csv
    // 6.1.2 aggregate_daily_sales_[dd_MM_yyyy].csv
    private static void exportAggregateToCSV() throws Exception {
        LoggerUtil.log("Đang xuất 2 file CSV tổng hợp...");
        exportDailySummaryCSV(); // 1.1
        exportDailySalesCSV(); // 1.2
        LoggerUtil.log("Xuất CSV xong rồi ạ!");
    }
    // 6.1.1 Xuất file aggregate_daily_summary (tổng hợp theo ngày)
    private static void exportDailySummaryCSV() throws Exception {
        String sql = """
                SELECT d.full_date,
                       COUNT(DISTINCT f.product_id),
                       COALESCE(AVG(f.price),0),
                       COALESCE(MIN(f.price),0),
                       COALESCE(MAX(f.price),0)
                FROM warehouse_db.fact_product_price_daily f
                JOIN warehouse_db.dim_date d ON f.date_key = d.date_key
                GROUP BY d.full_date ORDER BY d.full_date DESC""";
        String file = AGGREGATE_DIR + "/aggregate_daily_summary_" + DATE_STR + ".csv";
        writeCsv(sql, file, new String[]{"full_date", "num_products", "avg_price", "min_price", "max_price"});
    }

    // 6.1.2 Xuất file aggregate_daily_sales (chi tiết từng sản phẩm theo ngày)
    private static void exportDailySalesCSV() throws Exception {
        String sql = """
                SELECT d.full_date, p.product_name, p.brand,
                       COALESCE(AVG(f.price),0),
                       COALESCE(MIN(f.price),0),
                       COALESCE(MAX(f.price),0)
                FROM warehouse_db.fact_product_price_daily f
                JOIN warehouse_db.dim_date d ON f.date_key = d.date_key
                JOIN warehouse_db.dim_product p ON f.product_id = p.product_id
                GROUP BY d.full_date, p.product_id
                ORDER BY d.full_date DESC, 4 DESC""";
        String file = AGGREGATE_DIR + "/aggregate_daily_sales_" + DATE_STR + ".csv";
        writeCsv(sql, file, new String[]{"full_date", "product_name", "brand", "avg_price", "min_price", "max_price"});
    }

    // 6.2. Tạo và nạp dữ liệu cho 5 bảng Mart trong schema data_mart

    // 6.2.1 Tạo bảng mart_daily_summary → Tổng hợp giá theo ngày
    private static void createAndLoadMartDailySummary() throws Exception {
        execute("DROP TABLE IF EXISTS data_mart.mart_daily_summary",
                "CREATE TABLE data_mart.mart_daily_summary (" +
                        "full_date DATE PRIMARY KEY," +
                        "num_products INT," +
                        "avg_price DECIMAL(15,2)," +
                        "min_price DECIMAL(15,2)," +
                        "max_price DECIMAL(15,2)" +
                        ")",
                "INSERT INTO data_mart.mart_daily_summary " +
                        "SELECT d.full_date," +
                        "       COUNT(DISTINCT f.product_id)," +
                        "       COALESCE(AVG(f.price),0)," +
                        "       COALESCE(MIN(f.price),0)," +
                        "       COALESCE(MAX(f.price),0) " +
                        "FROM warehouse_db.fact_product_price_daily f " +
                        "JOIN warehouse_db.dim_date d ON f.date_key = d.date_key " +
                        "GROUP BY d.full_date",
                "mart_daily_summary");
        LoggerUtil.log("ĐÃ TẠO VÀ LOAD BẢNG mart_daily_summary");
    }
    // 6.2.2 Tạo bảng mart_daily_sales → Chi tiết giá từng sản phẩm theo ngày
    private static void createAndLoadMartDailySales() throws Exception {
        execute("DROP TABLE IF EXISTS data_mart.mart_daily_sales",
                "CREATE TABLE data_mart.mart_daily_sales (" +
                        "full_date DATE, product_id INT, product_name VARCHAR(255), brand VARCHAR(100)," +
                        "avg_price DECIMAL(15,2), min_price DECIMAL(15,2), max_price DECIMAL(15,2)," +
                        "PRIMARY KEY (full_date, product_id))",
                "INSERT INTO data_mart.mart_daily_sales " +
                        "SELECT d.full_date, p.product_id, p.product_name, p.brand," +
                        "COALESCE(AVG(f.price),0), COALESCE(MIN(f.price),0), COALESCE(MAX(f.price),0) " +
                        "FROM warehouse_db.fact_product_price_daily f " +
                        "JOIN warehouse_db.dim_date d ON f.date_key = d.date_key " +
                        "JOIN warehouse_db.dim_product p ON f.product_id = p.product_id " +
                        "GROUP BY d.full_date, p.product_id",
                "mart_daily_sales");
    }

    // 6.2.3 Tạo bảng mart_product_summary → Giá trung bình 30 ngày của từng sản phẩm
    private static void createAndLoadMartProductSummary() throws Exception {
        execute("DROP TABLE IF EXISTS data_mart.mart_product_summary",
                "CREATE TABLE data_mart.mart_product_summary (" +
                        "product_id INT PRIMARY KEY, product_name VARCHAR(255), brand VARCHAR(100), avg_price_30days DECIMAL(15,2))",
                "INSERT INTO data_mart.mart_product_summary " +
                        "SELECT p.product_id, p.product_name, p.brand, COALESCE(AVG(f.price),0) " +
                        "FROM warehouse_db.dim_product p " +
                        "LEFT JOIN warehouse_db.fact_product_price_daily f ON p.product_id = f.product_id " +
                        "AND f.crawl_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) " +
                        "GROUP BY p.product_id",
                "mart_product_summary");
    }

    // 6.2.4 Tạo bảng mart_top10_daily_change → Top 10 sản phẩm thay đổi giá mạnh nhất
    private static void createAndLoadTop10DailyChange() throws Exception {
        String insertSql = """
                INSERT INTO data_mart.mart_top10_daily_change
                WITH all_prices AS (
                    SELECT 
                        f.product_id,
                        d.full_date,
                        f.price,
                        LAG(f.price) OVER (PARTITION BY f.product_id ORDER BY d.full_date) AS prev_price
                    FROM warehouse_db.fact_product_price_daily f
                    JOIN warehouse_db.dim_date d ON f.date_key = d.date_key
                ),
                price_changes AS (
                    SELECT 
                        full_date AS change_date,
                        product_id,
                        prev_price,
                        price AS current_price,
                        ROUND((price - prev_price) / prev_price * 100, 2) AS percent_change
                    FROM all_prices
                    WHERE prev_price IS NOT NULL AND price != prev_price
                ),
                latest_change_date AS (
                    SELECT MAX(change_date) AS latest_date
                    FROM price_changes
                ),
                ranked_changes AS (
                    SELECT 
                        pc.change_date,
                        p.product_name,
                        p.brand,
                        pc.prev_price,
                        pc.current_price,
                        pc.percent_change,
                        ROW_NUMBER() OVER (ORDER BY ABS(pc.percent_change) DESC) AS top_rank
                    FROM price_changes pc
                    JOIN warehouse_db.dim_product p ON pc.product_id = p.product_id
                    CROSS JOIN latest_change_date ld
                    WHERE pc.change_date = ld.latest_date
                )
                SELECT 
                    change_date AS full_date,
                    top_rank,
                    product_name,
                    brand,
                    prev_price,
                    current_price,
                    percent_change
                FROM ranked_changes
                WHERE top_rank <= 10
                ORDER BY top_rank
                """;

        execute(
                "DROP TABLE IF EXISTS data_mart.mart_top10_daily_change",
                """
                CREATE TABLE data_mart.mart_top10_daily_change (
                    full_date DATE,
                    top_rank TINYINT,
                    product_name VARCHAR(255),
                    brand VARCHAR(100),
                    prev_price DECIMAL(15,2),
                    current_price DECIMAL(15,2),
                    percent_change DECIMAL(6,2),
                    PRIMARY KEY (full_date, top_rank)
                )
                """,
                insertSql,
                "mart_top10_daily_change"
        );
    }

    // 6.2.5 Tạo bảng mart_today_top_price → Top 20 sản phẩm đắt nhất hôm nay
    private static void createAndLoadTodayTopPrice() throws Exception {
        execute("DROP TABLE IF EXISTS data_mart.mart_today_top_price",
                """
                CREATE TABLE data_mart.mart_today_top_price (
                    rank_no TINYINT PRIMARY KEY,
                    product_name VARCHAR(255),
                    brand VARCHAR(100),
                    today_price DECIMAL(15,2)
                )
                """,
                """
                INSERT INTO data_mart.mart_today_top_price
                WITH ranked_today AS (
                    SELECT 
                        p.product_name,
                        p.brand,
                        ds.avg_price AS today_price,
                        ROW_NUMBER() OVER (ORDER BY ds.avg_price DESC) AS rank_no
                    FROM data_mart.mart_daily_sales ds
                    JOIN data_mart.dim_product p ON ds.product_id = p.product_id
                    WHERE ds.full_date = CURDATE()
                )
                SELECT rank_no, product_name, brand, today_price
                FROM ranked_today
                WHERE rank_no <= 20
                ON DUPLICATE KEY UPDATE
                    product_name = VALUES(product_name),
                    brand = VALUES(brand),
                    today_price = VALUES(today_price)
                """,
                "mart_today_top_price");
        LoggerUtil.log("ĐÃ TẠO BẢNG THỨ 5: mart_today_top_price – TOP 20 SẢN PHẨM ĐẮT NHẤT HÔM NAY");
    }

    // 6.3. Sao chép 2 bảng chiều từ warehouse_db sang data_mart

    // 6.3.1 Sao chép bảng dim_date
    private static void copyDimDate() throws Exception {
        execute("DROP TABLE IF EXISTS data_mart.dim_date",
                "CREATE TABLE data_mart.dim_date LIKE warehouse_db.dim_date",
                "INSERT INTO data_mart.dim_date SELECT * FROM warehouse_db.dim_date",
                "dim_date");
    }
    // 6.3.2 Sao chép bảng dim_product
    private static void copyDimProduct() throws Exception {
        execute("DROP TABLE IF EXISTS data_mart.dim_product",
                "CREATE TABLE data_mart.dim_product LIKE warehouse_db.dim_product",
                "INSERT INTO data_mart.dim_product SELECT * FROM warehouse_db.dim_product",
                "dim_product");
    }

    private static void writeCsv(String sql, String file, String[] header) throws Exception {
        try (Connection c = WarehouseDBConfig.getConnection();
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery(sql);
             CSVWriter w = new CSVWriter(new FileWriter(file))) {
            w.writeNext(header);
            int count = 0;
            while (rs.next()) {
                String[] row = new String[header.length];
                for (int i = 0; i < header.length; i++) row[i] = rs.getString(i + 1);
                w.writeNext(row);
                count++;
            }
            LoggerUtil.log("Exported: " + file + " (" + count + " dòng)");
        }
    }

    private static void execute(String sql1, String sql2, String sql3, String table) throws Exception {
        try (Connection c = DataMartDBConfig.getConnection(); Statement s = c.createStatement()) {
            s.execute(sql1);
            s.execute(sql2);
            int rows = s.executeUpdate(sql3);
            LoggerUtil.log("Loaded data_mart." + table + ": " + rows + " rows");
        }
    }

    // 6.5. Xuất file log chi tiết
    private static void exportLogToFile() {
        String logDir = "data/log";
        String logFileName = "Script6_LoadToMart_" + DATE_STR + ".log";
        String fullPath = logDir + "/" + logFileName;
        try {
            Files.createDirectories(Paths.get(logDir));
            String fullLog = LoggerUtil.getFullLog();
            Files.writeString(Paths.get(fullPath), fullLog, StandardCharsets.UTF_8);
            LoggerUtil.log("Đã xuất file log: " + fullPath);
        } catch (Exception e) {
            LoggerUtil.log("Lỗi xuất log: " + e.getMessage());
        }
    }

    // 6.4. Tạo Dashboard phân tích
    private static void generateDashboard() {
        LoggerUtil.log("ĐANG TẠO DASHBOARD – ĐỦ 7 BẢNG TRONG DATA MART");

        StringBuilder trendLabels = new StringBuilder();
        StringBuilder trendData = new StringBuilder();
        StringBuilder top10Html = new StringBuilder();
        StringBuilder productSummaryHtml = new StringBuilder();
        StringBuilder todayTopPriceHtml = new StringBuilder();
        StringBuilder brandLabels = new StringBuilder();
        StringBuilder brandData = new StringBuilder();

        try (Connection conn = DataMartDBConfig.getConnection()) {

            //  Xu hướng giá
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT DATE_FORMAT(full_date, '%d/%m') AS d, avg_price FROM data_mart.mart_daily_summary ORDER BY full_date DESC LIMIT 30");
                 ResultSet rs = ps.executeQuery()) {
                boolean first = true;
                while (rs.next()) {
                    if (!first) { trendLabels.append(","); trendData.append(","); }
                    trendLabels.append("'").append(rs.getString("d")).append("'");
                    trendData.append(rs.getBigDecimal("avg_price").setScale(0, RoundingMode.HALF_UP));
                    first = false;
                }
                if (trendLabels.length() == 0) { trendLabels.append("'Chưa có'"); trendData.append("0"); }
            }

            //  Top 10 thay đổi giá
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT top_rank, product_name, brand, prev_price, current_price, percent_change FROM data_mart.mart_top10_daily_change ORDER BY top_rank");
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    int rank = rs.getInt("top_rank");
                    String rankClass = rank == 1 ? "rank1" : rank == 2 ? "rank2" : rank == 3 ? "rank3" : "";
                    BigDecimal pct = rs.getBigDecimal("percent_change");
                    String sign = pct.compareTo(BigDecimal.ZERO) > 0 ? "+" : "";
                    String color = pct.compareTo(BigDecimal.ZERO) > 0 ? "up" : "down";
                    top10Html.append("<tr class=\"").append(rankClass).append("\">")
                            .append("<td>").append(rank).append("</td>")
                            .append("<td>").append(escape(rs.getString("product_name"))).append("</td>")
                            .append("<td>").append(rs.getString("brand")).append("</td>")
                            .append("<td>").append(String.format("%,.0f", rs.getBigDecimal("prev_price"))).append(" ₫</td>")
                            .append("<td>").append(String.format("%,.0f", rs.getBigDecimal("current_price"))).append(" ₫</td>")
                            .append("<td class=\"").append(color).append("\">").append(sign).append(pct.setScale(2, RoundingMode.HALF_UP)).append("%</td>")
                            .append("</tr>");
                }
            }

            //  Giá TB 30 ngày
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT product_name, brand, FORMAT(avg_price_30days, 0) AS price FROM data_mart.mart_product_summary ORDER BY avg_price_30days DESC LIMIT 20");
                 ResultSet rs = ps.executeQuery()) {
                int stt = 1;
                while (rs.next()) {
                    productSummaryHtml.append("<tr><td>").append(stt++).append("</td><td>").append(escape(rs.getString("product_name"))).append("</td><td>").append(rs.getString("brand")).append("</td><td>").append(rs.getString("price")).append(" ₫</td></tr>");
                }
            }

            //  Top 20 đắt nhất hôm nay – từ bảng mart_today_top_price
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT rank_no, product_name, brand, FORMAT(today_price, 0) AS price FROM data_mart.mart_today_top_price ORDER BY rank_no");
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    int rank = rs.getInt("rank_no");
                    String rankClass = rank == 1 ? "rank1" : rank == 2 ? "rank2" : rank == 3 ? "rank3" : "";
                    todayTopPriceHtml.append("<tr class=\"").append(rankClass).append("\">")
                            .append("<td>").append(rank).append("</td>")
                            .append("<td>").append(escape(rs.getString("product_name"))).append("</td>")
                            .append("<td>").append(rs.getString("brand")).append("</td>")
                            .append("<td>").append(rs.getString("price")).append(" ₫</td>")
                            .append("</tr>");
                }
            }

            //  Biểu đồ theo hãng
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT brand, COUNT(*) c FROM data_mart.dim_product GROUP BY brand ORDER BY c DESC LIMIT 8");
                 ResultSet rs = ps.executeQuery()) {
                boolean first = true;
                while (rs.next()) {
                    if (!first) { brandLabels.append(","); brandData.append(","); }
                    brandLabels.append("'").append(rs.getString("brand")).append("'");
                    brandData.append(rs.getInt("c"));
                    first = false;
                }
            }

        } catch (Exception e) {
            LoggerUtil.log("LỖI LẤY DỮ LIỆU DASHBOARD: " + e.getMessage());
            e.printStackTrace();
        }

        String timeNow = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(new Date());
        String html = "<!DOCTYPE html><html lang=\"vi\"><head><meta charset=\"UTF-8\">" +
                "<title>CELLPHONES DATA MART - 10 ĐIỂM ĐỎ</title>" +
                "<script src=\"https://cdn.jsdelivr.net/npm/chart.js\"></script>" +
                "<style>body{font-family:Segoe UI;background:#0f2027;color:white;margin:0;padding:20px;}" +
                ".c{max-width:1700px;margin:auto;background:rgba(0,0,0,0.9);padding:50px;border-radius:30px;box-shadow:0 0 70px #00e676;}" +
                "h1,h2{text-align:center;color:#00e676;text-shadow:0 0 30px #00e676;}h2{color:gold;font-size:2.5em;}" +
                ".time{color:#ff1744;font-size:2em;text-align:center;margin:20px;font-weight:bold;}" +
                "canvas{background:rgba(255,255,255,0.1);border-radius:20px;padding:20px;margin:40px 0;}" +
                "table{width:100%;border-collapse:collapse;margin:50px 0;font-size:1.5em;background:rgba(0,0,0,0.7);border-radius:15px;overflow:hidden;}" +
                "th{background:#c62828;padding:18px;color:white;}td{padding:15px;text-align:center;}" +
                ".rank1{background:gold!important;color:black!important;font-weight:bold;}" +
                ".rank2{background:silver!important;color:black!important;}.rank3{background:#cd7f32!important;color:white!important;}" +
                ".up{color:#00e676;font-weight:bold;}.down{color:#ff1744;font-weight:bold;}" +
                ".footer{text-align:center;margin:100px 0;font-size:2.5em;color:#00e676;font-weight:bold;}</style></head>" +
                "<body><div class=\"c\">" +
                "<h1>CELLPHONES DATA MART</h1><h2>ĐỒ ÁN DATA WAREHOUSE - NHÓM 12</h2>" +
                "<div class=\"time\">Cập nhật: " + timeNow + "</div>" +
                "<canvas id=\"trend\"></canvas><canvas id=\"brand\"></canvas>" +
                "<h2>TOP 10 SẢN PHẨM THAY ĐỔI GIÁ MẠNH NHẤT</h2><table><tr><th>Hạng</th><th>Sản phẩm</th><th>Hãng</th><th>Giá cũ</th><th>Giá mới</th><th>±%</th></tr>" + top10Html + "</table>" +
                "<h2>GIÁ TRUNG BÌNH 30 NGÀY (TOP 20)</h2><table><tr><th>STT</th><th>Sản phẩm</th><th>Hãng</th><th>Giá TB</th></tr>" + productSummaryHtml + "</table>" +
                "<h2>TOP 20 SẢN PHẨM ĐẮT NHẤT HÔM NAY</h2><table><tr><th>Hạng</th><th>Sản phẩm</th><th>Hãng</th><th>Giá hôm nay</th></tr>" + todayTopPriceHtml + "</table>" +
                "<div class=\"footer\">100% JAVA • 7 BẢNG TRONG DATA MART •</div></div>" +
                "<script>" +
                "new Chart(document.getElementById('trend'),{type:'line',data:{labels:[" + new StringBuilder(trendLabels).reverse() + "],datasets:[{label:'Giá trung bình',data:[" + new StringBuilder(trendData).reverse() + "],borderColor:'#00e676',backgroundColor:'rgba(0,230,118,0.3)',tension:0.4,fill:true}]},options:{plugins:{title:{display:true,text:'XU HƯỚNG GIÁ 30 NGÀY',font:{size:26}}}}});" +
                "new Chart(document.getElementById('brand'),{type:'doughnut',data:{labels:[" + brandLabels + "],datasets:[{data:[" + brandData + "],backgroundColor:['#ff1744','#00e676','#ffd600','#2196f3','#ff9800','#9c27b0','#00bcd4','#e91e63']}]},options:{plugins:{title:{display:true,text:'TỶ LỆ THEO HÃNG',font:{size:26}}}}});" +
                "</script></body></html>";

        try {
            Path path = Paths.get("dashboard/ui_mart.html");
            Files.createDirectories(path.getParent());
            Files.writeString(path, html, StandardCharsets.UTF_8);
            String abs = path.toAbsolutePath().toString();
            LoggerUtil.log("DASHBOARD TẠO XONG – 7 BẢNG HOÀN HẢO!");
            LoggerUtil.log("\u001B[32m>>> MỞ DASHBOARD: file:///" + abs.replace("\\", "/") + "\u001B[0m");
            if (System.getProperty("os.name").toLowerCase().contains("win")) {
                new ProcessBuilder("cmd", "/c", "start", "", abs).start();
            }
        } catch (Exception e) {
            LoggerUtil.log("LỖI GHI DASHBOARD: " + e.getMessage());
        }
    }

    private static String escape(String s) {
        return s == null ? "" : s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
    }

    public static void main(String[] args) {
        try {
            LoggerUtil.log("======================================================================");
            LoggerUtil.log("=                  BẮT ĐẦU CHẠY SCRIPT 6 - LOAD TO DATA MART          =");
            LoggerUtil.log("=                     Ngày chạy: " + new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(new Date()) + "                     =");
            LoggerUtil.log("======================================================================");

            //  Xuất 2 file CSV tổng hợp
            load();                    // ← toàn bộ quy trình nằm trong hàm load()
            exportLogToFile();         // 6.5. Xuất file log chi tiết
            generateDashboard();       // 6.4. Tạo Dashboard HTML

            System.out.println("\nHOÀN TẤT 100% – 7 BẢNG TRONG data_mart!");
            System.out.println("Dashboard: dashboard/ui_mart.html");

        } catch (Exception e) {
            LoggerUtil.log("SCRIPT 6 LỖI: " + e.getMessage());
            e.printStackTrace();
            exportLogToFile();
        }
    }
}