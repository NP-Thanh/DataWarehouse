package vn.edu.hcmuaf.fit.date_dim;

import com.opencsv.CSVReader;
import vn.edu.hcmuaf.fit.db.DatabaseConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;

public class LoadDateDim {

    public static void load(String csvFile) throws Exception {
        String sql = """
            INSERT INTO dim_date (
                date_key, full_date, day_of_month, month_number, day_name,
                month_name, year_number, year_month_label, day_of_year,
                day_in_month, week_of_year, week_year_label, week_start_date,
                quarter_number, quarter_year_label, quarter_start_date,
                holiday_flag, weekend_flag
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """;

        // Kết nối DB
        try (Connection conn = DatabaseConfig.getConnection()) {

            CSVReader reader = new CSVReader(new FileReader(csvFile));
            reader.readNext(); // bỏ header

            PreparedStatement ps = conn.prepareStatement(sql);

            String[] row;
            int count = 0;

            // Đọc từng dòng CSV
            while ((row = reader.readNext()) != null) {

                for (int i = 1; i <= 18; i++) {
                    ps.setString(i, row[i - 1]);  // row[0] -> param 1
                }

                ps.addBatch();

                if (++count % 200 == 0) {
                    ps.executeBatch();
                    LoggerUtil.log("Đã import " + count + " dòng date dim...");
                }
            }

            ps.executeBatch();
            LoggerUtil.log("Import Date Dimension thành công. Tổng dòng: " + count);
        }
    }

    public static void main(String[] args) {
        System.out.println("[START] Import Date Dimension...");

        try {
            String csvFile = System.getProperty("user.dir") + "/date_dim_without_quarter.csv";

            System.out.println("[INFO] Loading file: " + csvFile);
            load(csvFile);

            System.out.println("[DONE] Date Dimension import completed.");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("[ERROR] Import failed: " + e.getMessage());
        }
    }
}

