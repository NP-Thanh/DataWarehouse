package vn.edu.hcmuaf.fit.aggregate;

import vn.edu.hcmuaf.fit.db.WarehouseDBConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.sql.*;

public class CreateAggregateDailySummary {

    public static int load() throws Exception {
        LoggerUtil.log("=== [Script 5.2a] Tạo bảng aggregate agg_daily_summary (GIÁ THẬT) ===");
        LoggerUtil.log("📌 GROUP BY: full_date");
        LoggerUtil.log("📊 Aggregation: COUNT(DISTINCT product), AVG/MIN/MAX(price)");

        String dropTable = "DROP TABLE IF EXISTS warehouse_db.agg_daily_summary";

        String createTable = """
            CREATE TABLE warehouse_db.agg_daily_summary (
                agg_daily_summary_key INT AUTO_INCREMENT PRIMARY KEY,
                full_date DATE NOT NULL UNIQUE,
                num_products_tracked INT DEFAULT 0,
                avg_price DECIMAL(15, 2) DEFAULT 0,
                min_price DECIMAL(15, 2) DEFAULT 0,
                max_price DECIMAL(15, 2) DEFAULT 0
            )
        """;

        String createAndLoadAgg = """
            INSERT INTO warehouse_db.agg_daily_summary (full_date, num_products_tracked, avg_price, min_price, max_price)
            SELECT 
                d.full_date,
                COUNT(DISTINCT f.product_id) as num_products_tracked,
                AVG(f.price) as avg_price,
                MIN(f.price) as min_price,
                MAX(f.price) as max_price
            FROM warehouse_db.fact_product_price_daily f
            INNER JOIN warehouse_db.dim_date d ON f.date_key = d.date_key
            GROUP BY d.full_date
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

            LoggerUtil.log("✅ Tạo và load agg_daily_summary hoàn tất:");
            LoggerUtil.log("   - Bản ghi được tạo: " + count + " rows");
            LoggerUtil.log("   - Dữ liệu: num_products_tracked + avg/min/max price theo full_date");
            LoggerUtil.log("   - Thời gian: " + duration + "ms (<1 giây)");

            LoggerUtil.logStep("5.2", "AggDailySummary", count, duration, "SUCCESS", null);
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            errorMsg = e.getMessage();
            LoggerUtil.log("❌ Lỗi Script 5.2: " + errorMsg);
            LoggerUtil.logStep("5.2", "AggDailySummary", count, duration, "FAILED", errorMsg);
            e.printStackTrace();
            throw e;
        }

        return count;
    }
}