package vn.edu.hcmuaf.fit.mart;

import vn.edu.hcmuaf.fit.db.DataMartDBConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.sql.Connection;
import java.sql.Statement;

/**
 * Script 6: Load từ warehouse aggregates vào data_mart (Presentation Layer)
 * SIMPLIFIED: Chỉ copy real data (NO product_key, NO SCD Type 2)
 */
public class LoadToMart {

    public static void load() throws Exception {
        String runId = null;
        int totalRecords = 0;
        String jobName = "Load to Data Mart (FINAL)";

        try {
            runId = LoggerUtil.startProcess(LoggerUtil.SOURCE_CELLPHONES_ID, jobName);
            LoggerUtil.log("=== SCRIPT 6: LOAD TO DATA MART ===");

            totalRecords += copyDimDate();
            totalRecords += copyDimProduct();
            totalRecords += loadMartDailySales();
            totalRecords += loadMartDailySummary();
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

    /**
     * 6.1 Copy dim_date (SELECT * - tất cả cột)
     */
    private static int copyDimDate() throws Exception {
        String drop = "DROP TABLE IF EXISTS data_mart.dim_date";
        String create = """
                CREATE TABLE data_mart.dim_date (
                    date_key INT PRIMARY KEY,
                    full_date DATE NOT NULL UNIQUE,
                    day_of_month INT,
                    month_number INT,
                    day_name VARCHAR(20),
                    month_name VARCHAR(20),
                    year_number INT,
                    year_month_label VARCHAR(20),
                    day_of_year INT,
                    day_in_month INT,
                    week_of_year INT,
                    week_year_label VARCHAR(20),
                    week_start_date DATE,
                    quarter_number INT,
                    quarter_year_label VARCHAR(20),
                    quarter_start_date DATE,
                    holiday_flag INT DEFAULT 0,
                    weekend_flag INT DEFAULT 0
                )
                """;
        String insert = "INSERT INTO data_mart.dim_date SELECT * FROM warehouse_db.dim_date";

        try (Connection conn = DataMartDBConfig.getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(drop);
            }
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(create);
            }
            try (Statement stmt = conn.createStatement()) {
                int rows = stmt.executeUpdate(insert);
                LoggerUtil.log("  ✓ Copy dim_date: " + rows + " records");
                return rows;
            }
        }
    }

    /**
     * 6.2 Copy dim_product (SIMPLIFIED - CHỌN 5 CỘT CỤ THỂ)
     * ⚠️ FIX: SELECT product_id, product_name, brand, url, image_url (NOT SELECT *)
     */
    private static int copyDimProduct() throws Exception {
        String drop = "DROP TABLE IF EXISTS data_mart.dim_product";
        String create = """
                CREATE TABLE data_mart.dim_product (
                    product_id INT PRIMARY KEY,
                    product_name VARCHAR(255) NOT NULL,
                    brand VARCHAR(100) NOT NULL,
                    url VARCHAR(500) NOT NULL UNIQUE,
                    image_url VARCHAR(500)
                )
                """;
        String insert = """
                INSERT INTO data_mart.dim_product (product_id, product_name, brand, url, image_url)
                SELECT product_id, product_name, brand, url, image_url FROM warehouse_db.dim_product
                """;

        try (Connection conn = DataMartDBConfig.getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(drop);
            }
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(create);
            }
            try (Statement stmt = conn.createStatement()) {
                int rows = stmt.executeUpdate(insert);
                LoggerUtil.log("  ✓ Copy dim_product (SIMPLIFIED): " + rows + " records");
                return rows;
            }
        }
    }

    /**
     * 6.3 Load mart_daily_sales
     */
    private static int loadMartDailySales() throws Exception {
        String drop = "DROP TABLE IF EXISTS data_mart.mart_daily_sales";
        String create = """
                CREATE TABLE data_mart.mart_daily_sales (
                    mart_daily_sales_key INT AUTO_INCREMENT PRIMARY KEY,
                    date_key INT NOT NULL,
                    product_id INT NOT NULL,
                    avg_price DECIMAL(15, 2) DEFAULT 0,
                    min_price DECIMAL(15, 2) DEFAULT 0,
                    max_price DECIMAL(15, 2) DEFAULT 0,
                    UNIQUE KEY (date_key, product_id),
                    FOREIGN KEY (date_key) REFERENCES data_mart.dim_date(date_key),
                    FOREIGN KEY (product_id) REFERENCES data_mart.dim_product(product_id)
                )
                """;
        String insert = """
                INSERT INTO data_mart.mart_daily_sales (date_key, product_id, avg_price, min_price, max_price)
                SELECT date_key, product_id, avg_price, min_price, max_price FROM warehouse_db.agg_daily_sales
                """;

        try (Connection conn = DataMartDBConfig.getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(drop);
            }
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(create);
            }
            try (Statement stmt = conn.createStatement()) {
                int rows = stmt.executeUpdate(insert);
                LoggerUtil.log("  ✓ Load mart_daily_sales: " + rows + " records");
                return rows;
            }
        }
    }

    /**
     * 6.4 Load mart_daily_summary
     */
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
                SELECT full_date, num_products_tracked, avg_price, min_price, max_price FROM warehouse_db.agg_daily_summary
                """;

        try (Connection conn = DataMartDBConfig.getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(drop);
            }
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(create);
            }
            try (Statement stmt = conn.createStatement()) {
                int rows = stmt.executeUpdate(insert);
                LoggerUtil.log("  ✓ Load mart_daily_summary: " + rows + " records");
                return rows;
            }
        }
    }

    /**
     * 6.5 Load mart_product_summary
     */
    private static int loadMartProductSummary() throws Exception {
        String drop = "DROP TABLE IF EXISTS data_mart.mart_product_summary";
        String create = """
                CREATE TABLE data_mart.mart_product_summary (
                    product_id INT PRIMARY KEY,
                    product_name VARCHAR(255) NOT NULL,
                    brand VARCHAR(100) NOT NULL,
                    avg_price DECIMAL(15, 2) DEFAULT 0,
                    FOREIGN KEY (product_id) REFERENCES data_mart.dim_product(product_id)
                )
                """;
        String insert = """
                INSERT INTO data_mart.mart_product_summary (product_id, product_name, brand, avg_price)
                SELECT p.product_id, p.product_name, p.brand, COALESCE(AVG(f.price), 0)
                FROM data_mart.dim_product p
                LEFT JOIN warehouse_db.fact_product_price_daily f 
                    ON p.product_id = f.product_id 
                    AND f.crawl_date >= DATE(NOW()) - INTERVAL 30 DAY
                GROUP BY p.product_id, p.product_name, p.brand
                """;

        try (Connection conn = DataMartDBConfig.getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(drop);
            }
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(create);
            }
            try (Statement stmt = conn.createStatement()) {
                int rows = stmt.executeUpdate(insert);
                LoggerUtil.log("  ✓ Load mart_product_summary: " + rows + " records");
                return rows;
            }
        }
    }
}