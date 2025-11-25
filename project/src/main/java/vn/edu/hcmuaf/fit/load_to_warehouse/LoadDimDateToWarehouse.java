package vn.edu.hcmuaf.fit.load_to_warehouse;

import vn.edu.hcmuaf.fit.db.WarehouseDBConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.sql.*;

public class LoadDimDateToWarehouse {

    public static int load() throws Exception {
        LoggerUtil.log("=== [Script 4.1] Bắt đầu load dim_date từ staging_db sang warehouse_db ===");

        String insertToWarehouse = """
            INSERT IGNORE INTO warehouse_db.dim_date (
                date_key, full_date, day_of_month, month_number, day_name,
                month_name, year_number, year_month_label, day_of_year,
                day_in_month, week_of_year, week_year_label, week_start_date,
                quarter_number, quarter_year_label, quarter_start_date,
                holiday_flag, weekend_flag
            )
            SELECT 
                date_key, full_date, day_of_month, month_number, day_name,
                month_name, year_number, year_month_label, day_of_year,
                day_in_month, week_of_year, week_year_label, week_start_date,
                quarter_number, 
                CASE WHEN LENGTH(quarter_year_label) > 20 
                     THEN SUBSTRING(quarter_year_label, 1, 20) 
                     ELSE quarter_year_label 
                END,
                quarter_start_date,
                CASE WHEN holiday_flag = 'Holiday' THEN 1 ELSE 0 END,
                CASE WHEN weekend_flag = 'Weekend' THEN 1 ELSE 0 END
            FROM staging_db.dim_date
        """;

        int count = 0;
        long startTime = System.currentTimeMillis();
        String errorMsg = null;

        try (Connection warehouseConn = WarehouseDBConfig.getConnection()) {
            LoggerUtil.log("⚡ Dùng BULK INSERT - 1 lệnh SQL thay vì loop 7000+ lần...");

            try (Statement stmt = warehouseConn.createStatement()) {
                count = stmt.executeUpdate(insertToWarehouse);
            }

            long duration = System.currentTimeMillis() - startTime;

            LoggerUtil.log("✅ Load dim_date hoàn tất:");
            LoggerUtil.log("   - Bản ghi được insert: " + count);
            LoggerUtil.log("   - Thời gian: " + duration + "ms (~1-2 giây)");

            LoggerUtil.logStep("4.1", "LoadDimDate", count, duration, "SUCCESS", null);
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            errorMsg = e.getMessage();
            LoggerUtil.log("❌ Lỗi Script 4.1: " + errorMsg);
            LoggerUtil.logStep("4.1", "LoadDimDate", count, duration, "FAILED", errorMsg);
            e.printStackTrace();
            throw e;
        }

        return count;
    }
}