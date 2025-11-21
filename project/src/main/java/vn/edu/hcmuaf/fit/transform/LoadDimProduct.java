package vn.edu.hcmuaf.fit.transform;

import vn.edu.hcmuaf.fit.db.DatabaseConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.sql.*;

public class LoadDimProduct {

    public static int loadDim() throws Exception {
        String selectClean = """
                    SELECT DISTINCT product_name, brand, url, image_url
                    FROM stg_products_clean
                """;

        String checkExist = """
                    SELECT product_key
                    FROM dim_product
                    WHERE product_name = ? AND brand = ?
                """;

        String insertDim = """
                    INSERT INTO dim_product (product_name, brand, url, image_url)
                    VALUES (?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE product_key = product_key
                """;
        int count = 0;

        try (Connection conn = DatabaseConfig.getConnection()) {
            PreparedStatement psSelect = conn.prepareStatement(selectClean);
            ResultSet rs = psSelect.executeQuery();

            PreparedStatement psCheck = conn.prepareStatement(checkExist);
            PreparedStatement psInsert = conn.prepareStatement(insertDim);

            while (rs.next()) {
                String name = rs.getString("product_name");
                String brand = rs.getString("brand");
                String url = rs.getString("url");
                String img = rs.getString("image_url");

                // Check trùng
                psCheck.setString(1, name);
                psCheck.setString(2, brand);
                ResultSet rsCheck = psCheck.executeQuery();

                if (!rsCheck.next()) {
                    psInsert.setString(1, name);
                    psInsert.setString(2, brand);
                    psInsert.setString(3, url);
                    psInsert.setString(4, img);
                    psInsert.addBatch();
                    count++;
                }
            }

            psInsert.executeBatch();
            LoggerUtil.log("Dim product inserted: " + count + " new records.");
        }
        return count;
    }

    public static void main(String[] args) {
        String runId = null;
        int recordCount = 0;

        try {
            // 1. GHI LOG BẮT ĐẦU VÀO CONTROL DB
            runId = LoggerUtil.startProcess(LoggerUtil.SOURCE_CELLPHONES_ID, "Load dim_product");
            if (runId == null) {
                throw new Exception("Không thể khởi tạo Run ID hoặc kết nối Control DB bị lỗi.");
            }
            LoggerUtil.log("Bắt đầu thực thi Script 3");

            // 3. THỰC HIỆN TRANSFORM VÀ LOAD
            recordCount = loadDim();

            // 4. KẾT THÚC THÀNH CÔNG VÀ CẬP NHẬT CONTROL DB
            LoggerUtil.endProcess(recordCount, "SUCCESS", null);
            LoggerUtil.log("Load Dim Product hoàn tất. Tổng bản ghi được xử lý: " + recordCount);

        } catch (Exception e) {
            // 6. KẾT THÚC THẤT BẠI VÀ GHI LỖI VÀO CONTROL DB
            if (runId != null) {
                LoggerUtil.endProcess(recordCount, "FAILED", "Lỗi Load Dim Product: " + e.getMessage());
            }
            System.err.println("[ERROR] Load dim_product failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

