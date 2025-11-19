package vn.edu.hcmuaf.fit.transform;

import vn.edu.hcmuaf.fit.db.DatabaseConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;
import vn.edu.hcmuaf.fit.util.CleanUtil;

import java.sql.*;

public class LoadFactProductSnapshot {

    public static void loadFactSnapshot() throws Exception {

        String selectClean = """
            SELECT product_name, brand, price, original_price, crawl_date
            FROM stg_products_clean
        """;

        String selectProductKey = """
            SELECT product_key
            FROM dim_product
            WHERE product_name = ? AND brand = ?
        """;

        String selectDateKey = """
            SELECT date_key
            FROM dim_date
            WHERE full_date = ?
        """;

        String insertFact = """
            INSERT INTO fact_product_snapshot (product_key, price, original_price, date_key, crawl_date)
            VALUES (?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE price = VALUES(price), original_price = VALUES(original_price)
        """;

        try (Connection conn = DatabaseConfig.getConnection()) {
            PreparedStatement psSelect = conn.prepareStatement(selectClean);
            ResultSet rs = psSelect.executeQuery();

            PreparedStatement psProductKey = conn.prepareStatement(selectProductKey);
            PreparedStatement psDateKey = conn.prepareStatement(selectDateKey);
            PreparedStatement psInsert = conn.prepareStatement(insertFact);

            int count = 0;

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
                if (++count % 100 == 0) {
                    psInsert.executeBatch();
                    LoggerUtil.log("Inserted " + count + " fact records...");
                }
            }

            psInsert.executeBatch();
            LoggerUtil.log("Fact product snapshot inserted: " + count + " records.");
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[START] Load fact_product_snapshot process started...");
        try {
            loadFactSnapshot();
            System.out.println("[DONE] Load fact_product_snapshot completed successfully.");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("[ERROR] Load fact_product_snapshot failed: " + e.getMessage());
        }
    }
}

