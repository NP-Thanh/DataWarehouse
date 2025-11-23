package vn.edu.hcmuaf.fit.transform;

import vn.edu.hcmuaf.fit.db.DatabaseConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;
import vn.edu.hcmuaf.fit.util.CleanUtil;

import java.sql.*;

public class LoadFactProductSnapshot {

    public static int loadFactSnapshot() throws Exception {
        // SQL select dữ liệu từ bảng stg_products_clean
        String selectClean = """
            SELECT product_name, brand, price, original_price, crawl_date
            FROM stg_products_clean
        """;
        // SQL select key product từ bảng dim product
        String selectProductKey = """
            SELECT product_key
            FROM dim_product
            WHERE product_name = ? AND brand = ?
        """;
        // SQL select key date từ bảng dim date
        String selectDateKey = """
            SELECT date_key
            FROM dim_date
            WHERE full_date = ?
        """;
        // SQL insert dữ liệu vào bảng fact_product_snapshot
        String insertFact = """
            INSERT INTO fact_product_snapshot (product_key, price, original_price, date_key, crawl_date)
            VALUES (?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE price = VALUES(price), original_price = VALUES(original_price)
        """;
        // Biến đếm số dòng dữ liệu đã được thực thi
        int count=0;
        // Kết nối tới DB
        try (Connection conn = DatabaseConfig.getConnection()) {
            // Lấy dữ liệu từ bảng stg_products_clean
            PreparedStatement psSelect = conn.prepareStatement(selectClean);
            ResultSet rs = psSelect.executeQuery();
            // Lấy key product
            PreparedStatement psProductKey = conn.prepareStatement(selectProductKey);
            // Lấy key date
            PreparedStatement psDateKey = conn.prepareStatement(selectDateKey);
            // Insert dữ liệu vào bảng fact_product_snapshot
            PreparedStatement psInsert = conn.prepareStatement(insertFact);
            // Duyệt từng dòng
            while (rs.next()) {
                String name = rs.getString("product_name");
                String brand = rs.getString("brand");
                java.math.BigDecimal price = CleanUtil.cleanPrice(rs.getString("price"));
                java.math.BigDecimal original = CleanUtil.cleanPrice(rs.getString("original_price"));
                Date crawlDate = CleanUtil.cleanDate(rs.getString("crawl_date"));

                // Lấy product_key
                psProductKey.setString(1, name);
                psProductKey.setString(2, brand);
                ResultSet rsProduct = psProductKey.executeQuery();
                if (!rsProduct.next()) continue;
                int productKey = rsProduct.getInt("product_key");

                // Lấy date_key
                psDateKey.setDate(1, crawlDate);
                ResultSet rsDate = psDateKey.executeQuery();
                if (!rsDate.next()) continue;
                int dateKey = rsDate.getInt("date_key");

                // Insert vào fact
                psInsert.setInt(1, productKey);
                psInsert.setBigDecimal(2, price);
                psInsert.setBigDecimal(3, original);
                psInsert.setInt(4, dateKey);
                psInsert.setDate(5, crawlDate);

                psInsert.addBatch();
                count++;
            }
            // Thực thi batch
            psInsert.executeBatch();
            LoggerUtil.log("Fact product snapshot inserted: " + count + " records.");
        }
        return count;
    }

    public static void main(String[] args) {
        String runId = null;
        int recordCount = 0;

        try {
            // 1. GHI LOG BẮT ĐẦU VÀO CONTROL DB
            runId = LoggerUtil.startProcess(LoggerUtil.SOURCE_CELLPHONES_ID, "Load fact_product_snapshot");
            if (runId == null) {
                throw new Exception("Không thể khởi tạo Run ID hoặc kết nối Control DB bị lỗi.");
            }
            LoggerUtil.log("Bắt đầu thực thi Script 6: LOAD FACT PRODUCT SNAPSHOT.");

            // 3. THỰC HIỆN TRANSFORM VÀ LOAD
            recordCount = loadFactSnapshot();

            // 4. KẾT THÚC THÀNH CÔNG VÀ CẬP NHẬT CONTROL DB
            LoggerUtil.endProcess(recordCount, "SUCCESS", null);
            LoggerUtil.log("Load Fact Snapshot hoàn tất. Tổng bản ghi được xử lý: " + recordCount);
        } catch (Exception e) {
            // 6. KẾT THÚC THẤT BẠI VÀ GHI LỖI VÀO CONTROL DB
            if (runId != null) {
                LoggerUtil.endProcess(recordCount, "FAILED", "Lỗi Load Fact Snapshot: " + e.getMessage());
            }
            System.err.println("[ERROR] Load fact_product_snapshot failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

