package vn.edu.hcmuaf.fit.load_data;

import vn.edu.hcmuaf.fit.db.DatabaseConfig;
import vn.edu.hcmuaf.fit.util.CleanUtil;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class LoadCleanToDB {

    public static int load() throws Exception {

        String selectRaw = """
            SELECT product_name, brand, price, original_price, url, image_url, crawl_date
            FROM stg_products_raw
        """;

        String insertClean = """
            INSERT INTO stg_products_clean
            (product_name, brand, price, original_price, url, image_url, crawl_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """;

        int count = 0;
        int successfulInserts = 0;

        try (Connection conn = DatabaseConfig.getConnection()) {

            PreparedStatement psSelect = conn.prepareStatement(selectRaw);
            ResultSet rs = psSelect.executeQuery();

            PreparedStatement psInsert = conn.prepareStatement(insertClean);

            while (rs.next()) {
                // Làm sạch dữ liệu bằng CleanUtil
                String name = CleanUtil.cleanProductName(rs.getString("product_name"));
                String brand = rs.getString("brand");
                java.math.BigDecimal price = CleanUtil.cleanPrice(rs.getString("price"));
                java.math.BigDecimal original = CleanUtil.cleanPrice(rs.getString("original_price"));
                String url = rs.getString("url");
                String img = rs.getString("image_url");
                java.sql.Date crawlDate = CleanUtil.cleanDate(rs.getString("crawl_date"));

                psInsert.setString(1, name);
                psInsert.setString(2, brand);
                psInsert.setBigDecimal(3, price);
                psInsert.setBigDecimal(4, original);
                psInsert.setString(5, url);
                psInsert.setString(6, img);
                psInsert.setDate(7, crawlDate);

                psInsert.addBatch();
                count++;
            }

            psInsert.executeBatch();
            LoggerUtil.log("Clean hoàn tất: " + count + " bản ghi.");
        }
        return count;
    }

    public static void main(String[] args) {
        String runId = null;
        int recordCount = 0;

        try {
            // 1. GHI LOG BẮT ĐẦU VÀO CONTROL DB
            runId = LoggerUtil.startProcess(LoggerUtil.SOURCE_CELLPHONES_ID, "Clean data");
            if (runId == null) {
                throw new Exception("Không thể khởi tạo Run ID hoặc kết nối Control DB bị lỗi.");
            }
            LoggerUtil.log("Bắt đầu thực thi CLEAN DATA.");

            // 2. THỰC HIỆN CLEAN VÀ LOAD
            recordCount = load(); // Lấy số lượng bản ghi đã insert thành công

            // 3. KẾT THÚC THÀNH CÔNG VÀ CẬP NHẬT CONTROL DB
            LoggerUtil.endProcess(recordCount, "SUCCESS", null);
            LoggerUtil.log("✅ Clean & Load hoàn tất, tổng bản ghi sạch: " + recordCount);
        } catch (Exception e) {
            // 4. KẾT THÚC THẤT BẠI VÀ GHI LỖI VÀO CONTROL DB
            if (runId != null) {
                LoggerUtil.endProcess(recordCount, "FAILED", "Lỗi Clean Data: " + e.getMessage());
            }
            System.err.println("[ERROR] Clean failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
