package vn.edu.hcmuaf.fit.transform;

import vn.edu.hcmuaf.fit.db.StagingDBConfig;
import vn.edu.hcmuaf.fit.db.WarehouseDBConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.sql.*;

public class LoadDimDateToWarehouse {

    public static int load() throws Exception {
        LoggerUtil.log("=== [Script 4.1] Bắt đầu load dim_date từ staging_db sang warehouse_db ===");

        // ⚡ BULK INSERT - 1 lệnh SQL duy nhất thay vì loop 7670 lần
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

        try (Connection warehouseConn = WarehouseDBConfig.getConnection()) {
            LoggerUtil.log("⚡ Dùng BULK INSERT - 1 lệnh SQL thay vì loop 7000+ lần...");

            try (Statement stmt = warehouseConn.createStatement()) {
                count = stmt.executeUpdate(insertToWarehouse);
            }

            LoggerUtil.log("✅ Load dim_date hoàn tất. Tổng: " + count + " bản ghi. (Thời gian: ~5-10 giây)");
        } catch (Exception e) {
            LoggerUtil.log("❌ Lỗi Script 4.1: " + e.getMessage());
            throw e;
        }

        return count;
    }
}