package vn.edu.hcmuaf.fit.transform;

import vn.edu.hcmuaf.fit.db.WarehouseDBConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.sql.*;

public class CreateAggregateDailySales {

    public static int load() throws Exception {
        LoggerUtil.log("=== [Script 5.1a] Tạo bảng aggregate agg_daily_sales (GIÁ THẬT) ===");
        LoggerUtil.log("📌 GROUP BY: date_key, product_id");
        LoggerUtil.log("📊 Aggregation: AVG(price), MIN(price), MAX(price)");

        String dropTable = "DROP TABLE IF EXISTS warehouse_db.agg_daily_sales";

        String createTable = """
            CREATE TABLE warehouse_db.agg_daily_sales (
                agg_daily_sales_key INT AUTO_INCREMENT PRIMARY KEY,
                date_key INT NOT NULL,
                product_id INT NOT NULL,
                avg_price DECIMAL(15, 2) DEFAULT 0,
                min_price DECIMAL(15, 2) DEFAULT 0,
                max_price DECIMAL(15, 2) DEFAULT 0,
                UNIQUE KEY (date_key, product_id),
                FOREIGN KEY (date_key) REFERENCES warehouse_db.dim_date(date_key),
                FOREIGN KEY (product_id) REFERENCES warehouse_db.dim_product(product_id)
            )
        """;

        String createAndLoadAgg = """
            INSERT INTO warehouse_db.agg_daily_sales (date_key, product_id, avg_price, min_price, max_price)
            SELECT 
                f.date_key, 
                f.product_id, 
                AVG(f.price) as avg_price,
                MIN(f.price) as min_price,
                MAX(f.price) as max_price
            FROM warehouse_db.fact_product_price_daily f
            GROUP BY f.date_key, f.product_id
        """;

        int count = 0;
        long startTime = System.currentTimeMillis();
        String errorMsg = null;

        try (Connection warehouseConn = WarehouseDBConfig.getConnection()) {
            LoggerUtil.log("⚡ Dùng PURE SQL - DROP → CREATE → INSERT (NO JAVA)...");

            try (Statement stmt = warehouseConn.createStatement()) {
                stmt.executeUpdate(dropTable);
            }

            try (Statement stmt = warehouseConn.createStatement()) {
                stmt.executeUpdate(createTable);
            }

            try (Statement stmt = warehouseConn.createStatement()) {
                count = stmt.executeUpdate(createAndLoadAgg);
            }

            long duration = System.currentTimeMillis() - startTime;

            LoggerUtil.log("✅ Tạo và load agg_daily_sales hoàn tất:");
            LoggerUtil.log("   - Bản ghi được tạo: " + count + " rows");
            LoggerUtil.log("   - Dữ liệu: avg/min/max price theo date_key + product_id");
            LoggerUtil.log("   - Thời gian: " + duration + "ms (~1-2 giây)");

            LoggerUtil.logStep("5.1", "AggDailySales", count, duration, "SUCCESS", null);
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            errorMsg = e.getMessage();
            LoggerUtil.log("❌ Lỗi Script 5.1: " + errorMsg);
            LoggerUtil.logStep("5.1", "AggDailySales", count, duration, "FAILED", errorMsg);
            e.printStackTrace();
            throw e;
        }

        return count;
    }
}