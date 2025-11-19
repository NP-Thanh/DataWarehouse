package vn.edu.hcmuaf.fit.load_data;

import vn.edu.hcmuaf.fit.db.DatabaseConfig;
import vn.edu.hcmuaf.fit.util.CleanUtil;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class LoadCleanToDB {

    public static void load() throws Exception {

        String selectRaw = """
            SELECT product_name, brand, price, original_price, url, image_url, crawl_date
            FROM stg_products_raw
        """;

        String insertClean = """
            INSERT INTO stg_products_clean
            (product_name, brand, price, original_price, url, image_url, crawl_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """;

        try (Connection conn = DatabaseConfig.getConnection()) {

            PreparedStatement psSelect = conn.prepareStatement(selectRaw);
            ResultSet rs = psSelect.executeQuery();

            PreparedStatement psInsert = conn.prepareStatement(insertClean);

            int count = 0;

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

                if (++count % 100 == 0) {
                    psInsert.executeBatch();
                    LoggerUtil.log("Đã clean " + count + " dòng...");
                }
            }

            psInsert.executeBatch();
            LoggerUtil.log("Clean hoàn tất: " + count + " bản ghi.");
        }
    }

    public static void main(String[] args) {
        System.out.println("[START] Clean process started...");
        try {
            load();
            System.out.println("[DONE] Clean completed successfully.");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("[ERROR] Clean failed: " + e.getMessage());
        }
    }
}
