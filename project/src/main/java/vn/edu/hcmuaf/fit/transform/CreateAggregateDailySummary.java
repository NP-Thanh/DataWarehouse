package vn.edu.hcmuaf.fit.transform;

import vn.edu.hcmuaf.fit.db.WarehouseDBConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.sql.*;

public class CreateAggregateDailySummary {

    public static int load() throws Exception {
        LoggerUtil.log("=== [Script 5.2a] Tạo bảng aggregate agg_daily_summary trong warehouse_db ===");

        String createTable = """
            CREATE TABLE IF NOT EXISTS agg_daily_summary (
                agg_daily_summary_key INT AUTO_INCREMENT PRIMARY KEY,
                full_date DATE NOT NULL UNIQUE,
                total_products INT DEFAULT 0,
                total_revenue DECIMAL(15, 2) DEFAULT 0,
                total_units_sold INT DEFAULT 0,
                avg_price DECIMAL(15, 2) DEFAULT 0
            )
        """;

        String createAndLoadAgg = """
            INSERT INTO agg_daily_summary (full_date, total_products, total_revenue, total_units_sold, avg_price)
            SELECT 
                d.full_date,
                COUNT(DISTINCT f.product_key) as total_products,
                SUM(f.total_revenue) as total_revenue,
                SUM(f.units_sold) as total_units_sold,
                AVG(f.total_revenue / f.units_sold) as avg_price
            FROM fact_product_price_daily f
            INNER JOIN dim_date d ON f.date_key = d.date_key
            GROUP BY d.full_date
            ON DUPLICATE KEY UPDATE
                total_products = VALUES(total_products),
                total_revenue = VALUES(total_revenue),
                total_units_sold = VALUES(total_units_sold),
                avg_price = VALUES(avg_price)
        """;

        int count = 0;

        try (Connection warehouseConn = WarehouseDBConfig.getConnection()) {
            warehouseConn.setAutoCommit(false);

            PreparedStatement psCreateTable = warehouseConn.prepareStatement(createTable);
            psCreateTable.executeUpdate();
            psCreateTable.close();

            PreparedStatement psInsert = warehouseConn.prepareStatement(createAndLoadAgg);
            int result = psInsert.executeUpdate();
            warehouseConn.commit();

            count = result;
            LoggerUtil.log("✅ Tạo và load agg_daily_summary hoàn tất: " + count + " rows");
        }

        return count;
    }
}