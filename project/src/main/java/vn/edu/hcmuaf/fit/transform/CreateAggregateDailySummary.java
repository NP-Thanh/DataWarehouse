package vn.edu.hcmuaf.fit.transform;

import vn.edu.hcmuaf.fit.db.WarehouseDBConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.sql.*;

public class CreateAggregateDailySummary {

    public static int load() throws Exception {
        LoggerUtil.log("=== [Script 5.2a] Tạo bảng aggregate agg_daily_summary (GIÁ THẬT) ===");

        String dropTable = "DROP TABLE IF EXISTS agg_daily_summary";

        String createTable = """
            CREATE TABLE agg_daily_summary (
                agg_daily_summary_key INT AUTO_INCREMENT PRIMARY KEY,
                full_date DATE NOT NULL UNIQUE,
                num_products_tracked INT DEFAULT 0,
                avg_price DECIMAL(15, 2) DEFAULT 0,
                min_price DECIMAL(15, 2) DEFAULT 0,
                max_price DECIMAL(15, 2) DEFAULT 0
            )
        """;

        String createAndLoadAgg = """
            INSERT INTO agg_daily_summary (full_date, num_products_tracked, avg_price, min_price, max_price)
            SELECT 
                d.full_date,
                COUNT(DISTINCT f.product_key) as num_products_tracked,
                AVG(f.price) as avg_price,
                MIN(f.price) as min_price,
                MAX(f.price) as max_price
            FROM fact_product_price_daily f
            INNER JOIN dim_date d ON f.date_key = d.date_key
            GROUP BY d.full_date
        """;

        int count = 0;

        try (Connection warehouseConn = WarehouseDBConfig.getConnection()) {
            warehouseConn.setAutoCommit(false);

            // Drop existing table to ensure correct schema
            try (PreparedStatement psDrop = warehouseConn.prepareStatement(dropTable)) {
                psDrop.executeUpdate();
            }

            // Create fresh table
            try (PreparedStatement psCreateTable = warehouseConn.prepareStatement(createTable)) {
                psCreateTable.executeUpdate();
            }

            // Insert aggregated data
            try (PreparedStatement psInsert = warehouseConn.prepareStatement(createAndLoadAgg)) {
                int result = psInsert.executeUpdate();
                count = result;
            }

            warehouseConn.commit();
            LoggerUtil.log("✅ Tạo và load agg_daily_summary hoàn tất: " + count + " rows (avg/min/max price)");
        }

        return count;
    }
}