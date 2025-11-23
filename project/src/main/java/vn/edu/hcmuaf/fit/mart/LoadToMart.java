package vn.edu.hcmuaf.fit.mart;

import vn.edu.hcmuaf.fit.db.DataMartDBConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Script 6: Load từ warehouse aggregates vào data_mart
 * ONLY REAL DATA: avg_price, min_price, max_price (NO fake revenue/units_sold)
 */
public class LoadToMart {

    public static void load() throws Exception {
        String runId = null;
        int totalRecords = 0;
        String jobName = "Load to Data Mart (FINAL)";

        try {
            runId = LoggerUtil.startProcess(LoggerUtil.SOURCE_CELLPHONES_ID, jobName);
            LoggerUtil.log("=== SCRIPT 6: LOAD TO DATA MART ===");

            // 1. Copy dim_date (toàn bộ)
            totalRecords += copyDimDate();

            // 2. Copy dim_product (chỉ bản hiện tại - is_current = 1)
            totalRecords += copyDimProductCurrent();

            // 3. Load mart_daily_sales (ONLY real price data)
            totalRecords += loadMartDailySales();

            // 4. Load mart_daily_summary (ONLY real price data)
            totalRecords += loadMartDailySummary();

            // 5. Load mart_product_summary (ONLY real price data)
            totalRecords += loadMartProductSummary();

            LoggerUtil.endProcess(totalRecords, "SUCCESS", null);
            LoggerUtil.log("✓ Script 6 HOÀN THÀNH - Tổng: " + totalRecords + " bản ghi");
            LoggerUtil.log("✓ Data Mart sẵn sàng cho Power BI / Tableau!");

        } catch (Exception e) {
            if (runId != null) LoggerUtil.endProcess(totalRecords, "FAILED", e.getMessage());
            LoggerUtil.log("❌ LỖI LOAD TO MART: " + e.getMessage());
            throw e;
        }
    }

    private static int copyDimDate() throws Exception {
        String sql = """
                INSERT IGNORE INTO data_mart.dim_date
                SELECT * FROM warehouse_db.dim_date
                """;
        return execute(sql, "Copy dim_date");
    }

    private static int copyDimProductCurrent() throws Exception {
        String sql = """
                INSERT IGNORE INTO data_mart.dim_product
                SELECT * FROM warehouse_db.dim_product
                WHERE is_current = 1
                """;
        return execute(sql, "Copy dim_product (current only)");
    }

    private static int loadMartDailySales() throws Exception {
        String drop = "DROP TABLE IF EXISTS data_mart.mart_daily_sales";
        String create = """
                CREATE TABLE data_mart.mart_daily_sales (
                    mart_daily_sales_key INT AUTO_INCREMENT PRIMARY KEY,
                    date_key INT NOT NULL,
                    product_key VARCHAR(50) NOT NULL,
                    avg_price DECIMAL(15, 2) DEFAULT 0,
                    min_price DECIMAL(15, 2) DEFAULT 0,
                    max_price DECIMAL(15, 2) DEFAULT 0,
                    UNIQUE KEY (date_key, product_key)
                )
                """;
        String insert = """
                INSERT INTO data_mart.mart_daily_sales (date_key, product_key, avg_price, min_price, max_price)
                SELECT date_key, product_key, avg_price, min_price, max_price
                FROM warehouse_db.agg_daily_sales
                """;

        try (Connection conn = DataMartDBConfig.getConnection()) {
            conn.setAutoCommit(false);

            // Execute DROP separately
            try (PreparedStatement ps = conn.prepareStatement(drop)) {
                ps.executeUpdate();
            }

            // Execute CREATE separately
            try (PreparedStatement ps = conn.prepareStatement(create)) {
                ps.executeUpdate();
            }

            // Execute INSERT
            try (PreparedStatement ps = conn.prepareStatement(insert)) {
                int rows = ps.executeUpdate();
                conn.commit();
                LoggerUtil.log("  ✓ Load mart_daily_sales (price only): " + rows + " records");
                return rows;
            }
        }
    }

    private static int loadMartDailySummary() throws Exception {
        String drop = "DROP TABLE IF EXISTS data_mart.mart_daily_summary";
        String create = """
                CREATE TABLE data_mart.mart_daily_summary (
                    mart_daily_summary_key INT AUTO_INCREMENT PRIMARY KEY,
                    full_date DATE NOT NULL UNIQUE,
                    num_products_tracked INT DEFAULT 0,
                    avg_price DECIMAL(15, 2) DEFAULT 0,
                    min_price DECIMAL(15, 2) DEFAULT 0,
                    max_price DECIMAL(15, 2) DEFAULT 0
                )
                """;
        String insert = """
                INSERT INTO data_mart.mart_daily_summary (full_date, num_products_tracked, avg_price, min_price, max_price)
                SELECT full_date, num_products_tracked, avg_price, min_price, max_price
                FROM warehouse_db.agg_daily_summary
                """;

        try (Connection conn = DataMartDBConfig.getConnection()) {
            conn.setAutoCommit(false);

            // Execute DROP separately
            try (PreparedStatement ps = conn.prepareStatement(drop)) {
                ps.executeUpdate();
            }

            // Execute CREATE separately
            try (PreparedStatement ps = conn.prepareStatement(create)) {
                ps.executeUpdate();
            }

            // Execute INSERT
            try (PreparedStatement ps = conn.prepareStatement(insert)) {
                int rows = ps.executeUpdate();
                conn.commit();
                LoggerUtil.log("  ✓ Load mart_daily_summary (price only): " + rows + " records");
                return rows;
            }
        }
    }

    private static int loadMartProductSummary() throws Exception {
        String sql = """
                INSERT IGNORE INTO data_mart.mart_product_summary 
                (product_key, product_name, brand, avg_price)
                SELECT 
                    p.product_key,
                    p.product_name,
                    p.brand,
                    COALESCE(AVG(f.price), 0) as avg_price
                FROM warehouse_db.dim_product p
                LEFT JOIN warehouse_db.fact_product_price_daily f 
                    ON p.product_key = f.product_key 
                    AND f.crawl_date >= DATE(NOW()) - INTERVAL 30 DAY
                WHERE p.is_current = 1
                GROUP BY p.product_key, p.product_name, p.brand
                """;
        return execute(sql, "Load mart_product_summary (price only)");
    }

    private static int execute(String sql, String desc) throws SQLException {
        try (Connection conn = DataMartDBConfig.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            int rows = ps.executeUpdate();
            LoggerUtil.log("  ✓ " + desc + ": " + rows + " records");
            return rows;
        }
    }
}