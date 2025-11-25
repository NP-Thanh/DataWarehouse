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
        LoggerUtil.log("ĐANG TẠO DASHBOARD PHIÊN BẢN HOÀN HẢO – ĐẦY ĐỦ DỮ LIỆU – ĐẸP NHƯ POWER BI");

        StringBuilder trendLabels = new StringBuilder();
        StringBuilder trendData = new StringBuilder();
        StringBuilder top10Html = new StringBuilder();
        StringBuilder productSummaryHtml = new StringBuilder();
        StringBuilder todayTopPriceHtml = new StringBuilder();
        StringBuilder brandLabels = new StringBuilder();
        StringBuilder brandData = new StringBuilder();

        try (Connection conn = DataMartDBConfig.getConnection()) {

            // 1. Xu hướng giá trung bình 30 ngày (mới nhất trước)
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT DATE_FORMAT(full_date, '%d/%m') AS d, avg_price " +
                            "FROM data_mart.mart_daily_summary ORDER BY full_date DESC LIMIT 30");
                 ResultSet rs = ps.executeQuery()) {
                boolean first = true;
                while (rs.next()) {
                    if (!first) {
                        trendLabels.append(",");
                        trendData.append(",");
                    }
                    trendLabels.append("'").append(rs.getString("d")).append("'");
                    trendData.append(rs.getBigDecimal("avg_price").setScale(0, RoundingMode.HALF_UP));
                    first = false;
                }
            }

            // 2. Top 10 thay đổi giá mạnh nhất
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT top_rank, product_name, brand, prev_price, current_price, percent_change " +
                            "FROM data_mart.mart_top10_daily_change ORDER BY top_rank");
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
                            .append("<td>").append(String.format("%,.0f ₫", rs.getBigDecimal("prev_price"))).append("</td>")
                            .append("<td>").append(String.format("%,.0f ₫", rs.getBigDecimal("current_price"))).append("</td>")
                            .append("<td class=\"").append(color).append("\">")
                            .append(sign).append(pct.setScale(2, RoundingMode.HALF_UP)).append("%</td>")
                            .append("</tr>");
                }
            }

            // 3. Top 20 đắt nhất hôm nay
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT rank_no, product_name, brand, today_price " +
                            "FROM data_mart.mart_today_top_price ORDER BY rank_no");
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    int rank = rs.getInt("rank_no");
                    String rankClass = rank == 1 ? "rank1" : rank == 2 ? "rank2" : rank == 3 ? "rank3" : "";
                    todayTopPriceHtml.append("<tr class=\"").append(rankClass).append("\">")
                            .append("<td>").append(rank).append("</td>")
                            .append("<td>").append(escape(rs.getString("product_name"))).append("</td>")
                            .append("<td>").append(rs.getString("brand")).append("</td>")
                            .append("<td>").append(String.format("%,.0f ₫", rs.getBigDecimal("today_price"))).append("</td>")
                            .append("</tr>");
                }
            }

            // 4. Top 20 giá TB 30 ngày cao nhất
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT product_name, brand, avg_price_30days " +
                            "FROM data_mart.mart_product_summary ORDER BY avg_price_30days DESC LIMIT 20");
                 ResultSet rs = ps.executeQuery()) {
                int stt = 1;
                while (rs.next()) {
                    productSummaryHtml.append("<tr>")
                            .append("<td>").append(stt++).append("</td>")
                            .append("<td>").append(escape(rs.getString("product_name"))).append("</td>")
                            .append("<td>").append(rs.getString("brand")).append("</td>")
                            .append("<td>").append(String.format("%,.0f ₫", rs.getBigDecimal("avg_price_30days"))).append("</td>")
                            .append("</tr>");
                }
            }

            // 5. Tỷ lệ sản phẩm theo hãng
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT brand, COUNT(*) AS c FROM data_mart.dim_product GROUP BY brand ORDER BY c DESC LIMIT 8");
                 ResultSet rs = ps.executeQuery()) {
                boolean first = true;
                while (rs.next()) {
                    if (!first) {
                        brandLabels.append(",");
                        brandData.append(",");
                    }
                    brandLabels.append("'").append(rs.getString("brand")).append("'");
                    brandData.append(rs.getInt("c"));
                    first = false;
                }
            }

        } catch (Exception e) {
            LoggerUtil.log("LỖI KẾT NỐI HOẶC QUERY: " + e.getMessage());
            e.printStackTrace();
        }

        String timeNow = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(new Date());

        String htmlTemplate = """
                <!DOCTYPE html>
                <html lang="vi">
                <head>
                    <meta charset="UTF-8">
                    <meta name="viewport" content="width=device-width, initial-scale=1.0">
                    <title>Cellphones Data Mart - Nhóm 12</title>
                    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
                    <style>
                        body {font-family: 'Segoe UI', Arial, sans-serif; background: #f4f6f9; color: #333; margin:0; padding:0; line-height:1.6;}
                        .container {max-width: 1400px; margin: 0 auto; padding: 20px;}
                        .header {background: linear-gradient(135deg, #667eea, #764ba2); color: white; padding: 40px 20px; text-align: center; border-radius: 15px; box-shadow: 0 10px 30px rgba(0,0,0,0.2);}
                        h1 {margin:0; font-size: 3.5em; font-weight: 700;}
                       .time {font-size: 1.8em; margin-top: 10px; opacity: 0.9;}
                        .menu {background: white; padding: 20px; text-align: center; border-radius: 12px; box-shadow: 0 5px 20px rgba(0,0,0,0.1); margin: 30px 0; position: sticky; top: 10px; z-index: 100;}
                        .menu button {margin: 8px; padding: 14px 28px; font-size: 1.3em; background: #667eea; color: white; border: none; border-radius: 50px; cursor: pointer; transition: all 0.3s;}
                        .menu button:hover {background: #5a6fd8; transform: translateY(-3px); box-shadow: 0 8px 20px rgba(102,126,234,0.4);}
                        .section {background: white; margin: 40px 0; padding: 40px; border-radius: 15px; box-shadow: 0 8px 25px rgba(0,0,0,0.1);}
                        h2 {text-align: center; color: #444; font-size: 2.6em; margin-bottom: 30px;}
                        canvas {background: #fdfdfd; border-radius: 12px; padding: 20px; box-shadow: 0 5px 15px rgba(0,0,0,0.05);}
                        table {width: 100%%; border-collapse: collapse; margin: 30px 0; font-size: 1.35em;}
                        th {background: #667eea; color: white; padding: 20px; text-align: center;}
                        td {padding: 18px 15px; text-align: center; border-bottom: 1px solid #eee;}
                        tr:hover td {background: #f8f9ff;}
                        tr:nth-child(even) td {background: #fbfbff;}
                        .rank1 {background: #ffd700 !important; color: black; font-weight: bold;}
                        .rank2 {background: #c0c0c0 !important; color: black;}
                        .rank3 {background: #cd7f32 !important; color: black;}
                        .up {color: #e74c3c; font-weight: bold;}
                        .down {color: #27ae60; font-weight: bold;}
                        .footer {text-align: center; padding: 60px 20px; color: #777; font-size: 1.6em; background: #f8f9fa; margin-top: 50px; border-radius: 15px;}
                    </style>
                </head>
                <body>
                    <div class="container">
                        <div class="header">
                            <h1>CELLPHONES DATA MART</h1>
                            <div class="time">Cập nhật lúc: {{TIME}}</div>
                        </div>
                
                        <div class="menu">
                            <button onclick="location.href='#trend'">Xu hướng giá</button>
                            <button onclick="location.href='#brand'">Theo hãng</button>
                            <button onclick="location.href='#top10'">Top 10 giảm sốc</button>
                            <button onclick="location.href='#top20'">Top 20 đắt nhất</button>
                            <button onclick="location.href='#summary30'">Giá TB 30 ngày</button>
                        </div>
                
                        <div id="trend" class="section">
                            <h2>XU HƯỚNG GIÁ TRUNG BÌNH 30 NGÀY QUA</h2>
                            <canvas id="trendChart" height="100"></canvas>
                        </div>
                
                        <div id="brand" class="section">
                            <h2>TỶ LỆ SẢN PHẨM THEO HÃNG</h2>
                            <canvas id="brandChart" height="120"></canvas>
                        </div>
                
                        <div id="top10" class="section">
                            <h2>TOP 10 SẢN PHẨM THAY ĐỔI GIÁ MẠNH NHẤT</h2>
                            <div style="overflow-x:auto;">
                                <table><tr><th>Hạng</th><th>Sản phẩm</th><th>Hãng</th><th>Giá cũ</th><th>Giá mới</th><th>±%</th></tr>{{TOP10}}</table>
                            </div>
                        </div>
                
                        <div id="top20" class="section">
                            <h2>TOP 20 SẢN PHẨM ĐẮT NHẤT HÔM NAY</h2>
                            <div style="overflow-x:auto;">
                                <table><tr><th>Hạng</th><th>Sản phẩm</th><th>Hãng</th><th>Giá hôm nay</th></tr>{{TOP20}}</table>
                            </div>
                        </div>
                
                        <div id="summary30" class="section">
                            <h2>TOP 20 SẢN PHẨM GIÁ TB 30 NGÀY CAO NHẤT</h2>
                            <div style="overflow-x:auto;">
                                <table><tr><th>STT</th><th>Sản phẩm</th><th>Hãng</th><th>Giá TB 30 ngày</th></tr>{{SUMMARY30}}</table>
                            </div>
                        </div>
                
                        <div class="footer">100%% Java • 7 bảng Data Mart • Nhóm 12 • Đồ án Data Warehouse 2025</div>
                    </div>
                
                    <script>
                        new Chart(document.getElementById('trendChart'), {type:'line',data:{labels:[{{LABELS}}],datasets:[{label:'Giá trung bình (₫)',data:[{{DATA}}],borderColor:'#667eea',backgroundColor:'rgba(102,126,234,0.2)',tension:0.4,fill:true}]},options:{responsive:true,plugins:{title:{display:true,text:'Xu hướng giá 30 ngày',font:{size:20}}}}});
                        new Chart(document.getElementById('brandChart'), {type:'doughnut',data:{labels:[{{BRAND_LABELS}}],datasets:[{data:[{{BRAND_DATA}}],backgroundColor:['#667eea','#764ba2','#f093fb','#f5576c','#4facfe','#43e97b','#fad390','#ff9ff3']}]},options:{responsive:true,plugins:{title:{display:true,text:'Tỷ lệ theo hãng',font:{size:20}}}}});
                    </script>
                </body>
                </html>
                """;

        String finalHtml = htmlTemplate
                .replace("{{TIME}}", timeNow)
                .replace("{{TOP10}}", top10Html.length() > 0 ? top10Html.toString() : "<tr><td colspan=\"6\">Chưa có dữ liệu thay đổi giá</td></tr>")
                .replace("{{TOP20}}", todayTopPriceHtml.length() > 0 ? todayTopPriceHtml.toString() : "<tr><td colspan=\"4\">Chưa có dữ liệu</td></tr>")
                .replace("{{SUMMARY30}}", productSummaryHtml.length() > 0 ? productSummaryHtml.toString() : "<tr><td colspan=\"4\">Chưa có dữ liệu</td></tr>")
                .replace("{{LABELS}}", trendLabels.length() > 0 ? trendLabels.toString() : "'Chưa có dữ liệu'")
                .replace("{{DATA}}", trendData.length() > 0 ? trendData.toString() : "0")
                .replace("{{BRAND_LABELS}}", brandLabels.length() > 0 ? brandLabels.toString() : "'Chưa có'")
                .replace("{{BRAND_DATA}}", brandData.length() > 0 ? brandData.toString() : "0");

        try {
            Path path = Paths.get("dashboard/ui_mart.html");
            Files.createDirectories(path.getParent());
            Files.writeString(path, finalHtml, StandardCharsets.UTF_8);
            LoggerUtil.log("DASHBOARD HOÀN HẢO 100% – ĐÃ CÓ ĐẦY ĐỦ DỮ LIỆU VÀ BIỂU ĐỒ!");
            LoggerUtil.log("MỞ TRÌNH DUYỆT → http://localhost:8080");
        } catch (Exception e) {
            LoggerUtil.log("Lỗi ghi file dashboard: " + e.getMessage());
        }
    }

    private static String escape(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
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

            DashboardServer.startServer();

            System.out.println("\nHOÀN TẤT 100% – 7 BẢNG TRONG data_mart!");
            System.out.println("Dashboard: dashboard/ui_mart.html");

            // Giữ cửa sổ console mở mãi mãi để server không tắt
            new java.util.Scanner(System.in).nextLine();
        } catch (Exception e) {
            LoggerUtil.log("SCRIPT 6 LỖI: " + e.getMessage());
            e.printStackTrace();
            exportLogToFile();
        }
    }
}