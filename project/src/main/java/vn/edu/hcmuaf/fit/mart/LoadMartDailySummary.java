package vn.edu.hcmuaf.fit.mart;

import vn.edu.hcmuaf.fit.db.WarehouseDBConfig;
import vn.edu.hcmuaf.fit.db.DataMartDBConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.sql.*;

public class LoadMartDailySummary {

    public static void load() throws Exception {
        LoggerUtil.log("=== [Script 5.2] Bắt đầu load mart_daily_summary ===");

        String selectAggregate = """
            SELECT 
                d.full_date,
                COUNT(DISTINCT p.product_id) as total_products,
                SUM(f.total_revenue) as total_revenue,
                SUM(f.units_sold) as total_units_sold,
                AVG(p.current_price) as avg_price
            FROM fact_product_price_daily f
            INNER JOIN dim_date d ON f.date_key = d.date_key
            INNER JOIN dim_product p ON f.product_key = p.product_key
            GROUP BY d.full_date
            ORDER BY d.full_date
        """;

        String insertSummary = """
            INSERT INTO mart_daily_summary (
                full_date, total_products, total_revenue, 
                total_units_sold, avg_price
            )
            VALUES (?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE 
                total_products = VALUES(total_products),
                total_revenue = VALUES(total_revenue),
                total_units_sold = VALUES(total_units_sold),
                avg_price = VALUES(avg_price),
                created_at = CURRENT_TIMESTAMP
        """;

        try (Connection warehouseConn = WarehouseDBConfig.getConnection();
             Connection dataMartConn = DataMartDBConfig.getConnection()) {

            dataMartConn.setAutoCommit(false);

            PreparedStatement psSelect = warehouseConn.prepareStatement(selectAggregate);
            ResultSet rs = psSelect.executeQuery();

            PreparedStatement psInsert = dataMartConn.prepareStatement(insertSummary);

            int count = 0;

            while (rs.next()) {
                psInsert.setDate(1, rs.getDate("full_date"));
                psInsert.setInt(2, rs.getInt("total_products"));
                psInsert.setBigDecimal(3, rs.getBigDecimal("total_revenue"));
                psInsert.setInt(4, rs.getInt("total_units_sold"));
                psInsert.setBigDecimal(5, rs.getBigDecimal("avg_price"));

                psInsert.addBatch();

                if (++count % 100 == 0) {
                    psInsert.executeBatch();
                    dataMartConn.commit();
                    LoggerUtil.log("Đã tổng hợp " + count + " daily summary records...");
                }
            }

            psInsert.executeBatch();
            dataMartConn.commit();

            LoggerUtil.log("✅ Load mart_daily_summary hoàn tất. Tổng: " + count + " ngày");
        }
    }

    public static void main(String[] args) {
        try {
            load();
            System.out.println("[DONE] LoadMartDailySummary completed successfully.");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("[ERROR] LoadMartDailySummary failed: " + e.getMessage());
        }
    }
}
