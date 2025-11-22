package vn.edu.hcmuaf.fit.transform;

import vn.edu.hcmuaf.fit.db.StagingDBConfig;
import vn.edu.hcmuaf.fit.db.WarehouseDBConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.sql.*;

public class LoadDimDateToWarehouse {

    public static int load() throws Exception {
        LoggerUtil.log("=== [Script 4.1] Bắt đầu load dim_date từ staging_db sang warehouse_db ===");

        String selectFromStaging = """
            SELECT 
                date_key, full_date, day_of_month, month_number, day_name,
                month_name, year_number, year_month_label, day_of_year,
                day_in_month, week_of_year, week_year_label, week_start_date,
                quarter_number, quarter_year_label, quarter_start_date,
                holiday_flag, weekend_flag
            FROM dim_date
        """;

        String insertToWarehouse = """
            INSERT INTO dim_date (
                date_key, full_date, day_of_month, month_number, day_name,
                month_name, year_number, year_month_label, day_of_year,
                day_in_month, week_of_year, week_year_label, week_start_date,
                quarter_number, quarter_year_label, quarter_start_date,
                holiday_flag, weekend_flag
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE date_key = date_key
        """;

        int count = 0;

        try (Connection stagingConn = StagingDBConfig.getConnection();
             Connection warehouseConn = WarehouseDBConfig.getConnection()) {

            PreparedStatement psSelect = stagingConn.prepareStatement(selectFromStaging);
            ResultSet rs = psSelect.executeQuery();

            PreparedStatement psInsert = warehouseConn.prepareStatement(insertToWarehouse);

            while (rs.next()) {
                psInsert.setInt(1, rs.getInt("date_key"));
                psInsert.setDate(2, rs.getDate("full_date"));
                psInsert.setInt(3, rs.getInt("day_of_month"));
                psInsert.setInt(4, rs.getInt("month_number"));
                psInsert.setString(5, rs.getString("day_name"));
                psInsert.setString(6, rs.getString("month_name"));
                psInsert.setInt(7, rs.getInt("year_number"));
                psInsert.setString(8, rs.getString("year_month_label"));
                psInsert.setInt(9, rs.getInt("day_of_year"));
                psInsert.setInt(10, rs.getInt("day_in_month"));
                psInsert.setInt(11, rs.getInt("week_of_year"));
                psInsert.setString(12, rs.getString("week_year_label"));
                psInsert.setDate(13, rs.getDate("week_start_date"));
                psInsert.setInt(14, rs.getInt("quarter_number"));

                String quarterYearLabel = rs.getString("quarter_year_label");
                if (quarterYearLabel != null && quarterYearLabel.length() > 20) {
                    quarterYearLabel = quarterYearLabel.substring(0, 20);
                }
                psInsert.setString(15, quarterYearLabel);
                psInsert.setDate(16, rs.getDate("quarter_start_date"));

                String holidayFlag = rs.getString("holiday_flag");
                psInsert.setInt(17, "Holiday".equalsIgnoreCase(holidayFlag) ? 1 : 0);

                String weekendFlag = rs.getString("weekend_flag");
                psInsert.setInt(18, "Weekend".equalsIgnoreCase(weekendFlag) ? 1 : 0);

                psInsert.addBatch();
                count++;

                if (count % 500 == 0) {
                    psInsert.executeBatch();
                    LoggerUtil.log("Đã load " + count + " dòng dim_date...");
                }
            }

            psInsert.executeBatch();
            LoggerUtil.log("✅ Load dim_date hoàn tất. Tổng: " + count + " bản ghi.");
        }

        return count;
    }
}