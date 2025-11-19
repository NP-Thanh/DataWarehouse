package vn.edu.hcmuaf.fit.transform;

import vn.edu.hcmuaf.fit.db.DatabaseConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.sql.*;

public class LoadDimProduct {

    public static void loadDim() throws Exception {
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


        try (Connection conn = DatabaseConfig.getConnection()) {
            PreparedStatement psSelect = conn.prepareStatement(selectClean);
            ResultSet rs = psSelect.executeQuery();

            PreparedStatement psCheck = conn.prepareStatement(checkExist);
            PreparedStatement psInsert = conn.prepareStatement(insertDim);

            int count = 0;

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
                    if (++count % 100 == 0) {
                        psInsert.executeBatch();
                        LoggerUtil.log("Đã clean " + count + " dòng...");
                    }
                }
            }

            psInsert.executeBatch();
            LoggerUtil.log("Dim product inserted: " + count + " new records.");
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[START] Load dim_product process started...");
        try {
            loadDim();
            System.out.println("[DONE] Load dim_product completed successfully.");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("[ERROR] Load dim_product failed: " + e.getMessage());
        }
    }
}

