// File: LoadToMart.java (phiên bản HOÀN CHỈNH - em viết lại cho anh)
package vn.edu.hcmuaf.fit.mart;

import vn.edu.hcmuaf.fit.db.DataMartDBConfig;
import vn.edu.hcmuaf.fit.db.WarehouseDBConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class LoadToMart {

    public static void main(String[] args) {
        String runId = null;
        int totalRecords = 0;
        String jobName = "Load to Data Mart (FINAL)";

        try {
            runId = LoggerUtil.startProcess(LoggerUtil.SOURCE_CELLPHONES_ID, jobName);
            LoggerUtil.log("=== BẮT ĐẦU LOAD TO DATA MART - PHIÊN BẢN HOÀN CHỈNH ===");

            // 1. Copy dim_date (toàn bộ)
            totalRecords += copyDimDate();

            // 2. Copy dim_product (chỉ bản hiện tại - is_current = 1)
            totalRecords += copyDimProductCurrent();

            // 3. Load mart_daily_sales từ aggregate
            totalRecords += loadMartDailySales();

            // 4. Load mart_daily_summary từ aggregate
            totalRecords += loadMartDailySummary();

            // 5. Bonus: mart_product_summary (tổng hợp theo sản phẩm)
            totalRecords += loadMartProductSummary();

            LoggerUtil.endProcess(totalRecords, "SUCCESS", null);
            LoggerUtil.log("HOÀN TẤT DATA MART! Tổng bản ghi: " + totalRecords);
            LoggerUtil.log("Data Mart sẵn sàng cho Power BI / Tableau");

        } catch (Exception e) {
            if (runId != null) LoggerUtil.endProcess(totalRecords, "FAILED", e.getMessage());
            LoggerUtil.log("LỖI LOAD TO MART: " + e.getMessage());
            e.printStackTrace();
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
        String sql = """
                INSERT IGNORE INTO data_mart.mart_daily_sales (date_key, product_key, total_revenue, units_sold)
                SELECT date_key, product_key, total_revenue, units_sold
                FROM warehouse_db.agg_daily_sales
                """;
        return execute(sql, "Load mart_daily_sales");
    }

    private static int loadMartDailySummary() throws Exception {
        String sql = """
                INSERT IGNORE INTO data_mart.mart_daily_summary (full_date, total_products, total_revenue, total_units_sold, avg_price)
                SELECT full_date, total_products, total_revenue, total_units_sold, avg_price
                FROM warehouse_db.agg_daily_summary
                """;
        return execute(sql, "Load mart_daily_summary");
    }

    private static int loadMartProductSummary() throws Exception {
        String sql = """
                INSERT IGNORE INTO data_mart.mart_product_summary 
                (product_key, product_name, brand, total_revenue, total_units_sold, avg_price)
                SELECT 
                    p.product_key,
                    p.product_name,
                    p.brand,
                    COALESCE(SUM(f.total_revenue), 0),
                    COALESCE(SUM(f.units_sold), 0),
                    COALESCE(AVG(p.current_price), 0)
                FROM warehouse_db.dim_product p
                LEFT JOIN warehouse_db.fact_product_price_daily f ON p.product_key = f.product_key
                WHERE p.is_current = 1
                GROUP BY p.product_key, p.product_name, p.brand
                """;
        return execute(sql, "Load mart_product_summary (bonus)");
    }

    private static int execute(String sql, String desc) throws SQLException {
        try (Connection conn = DataMartDBConfig.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            int rows = ps.executeUpdate();
            LoggerUtil.log(desc + ": " + rows + " bản ghi");
            return rows;
        }
    }

    public static void load() throws Exception {
        main(null);  // Cho phép Main.java gọi LoadToMart.load()
    }
}