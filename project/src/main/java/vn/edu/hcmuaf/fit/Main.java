package vn.edu.hcmuaf.fit;

import vn.edu.hcmuaf.fit.crawl.Extract;
import vn.edu.hcmuaf.fit.db.DatabaseConfig;
import vn.edu.hcmuaf.fit.load_data.LoadToDB;
import vn.edu.hcmuaf.fit.util.LoggerUtil;
import vn.edu.hcmuaf.fit.transform.*;
import vn.edu.hcmuaf.fit.mart.*;

public class Main {
    public static void main(String[] args) {
        try {
            LoggerUtil.log("=== BẮT ĐẦU PIPELINE ===");

            String csvFile = Extract.crawlToCSV();
            LoggerUtil.log("✅ Crawl hoàn tất, file: " + csvFile);

            DatabaseConfig.createTableIfNotExists();
            LoggerUtil.log("✅ Bảng staging đã sẵn sàng.");

            LoadToDB.load(csvFile);
            LoggerUtil.log("✅ Dữ liệu đã được load vào staging.");
            LoggerUtil.log("=== TIẾP TỤC PIPELINE: SCRIPT 4 + SCRIPT 5 (WAREHOUSE & DATA MART) ===");

            // --------------------- SCRIPT 4: WAREHOUSE ---------------------
            LoadDimDateToWarehouse.load();
            LoggerUtil.log("✅ 4.1 dim_date → warehouse_db");

            LoadDimProductToWarehouse.load();
            LoggerUtil.log("✅ 4.2 dim_product (SCD Type 2) → warehouse_db");

            LoadFactProductPriceDaily.load();
            LoggerUtil.log("✅ 4.3 fact_product_price_daily → warehouse_db");

            // --------------------- SCRIPT 5: DATA MART ---------------------
            LoadMartDailySales.load();
            LoggerUtil.log("✅ 5.1 mart_daily_sales → data_mart");

            LoadMartDailySummary.load();
            LoggerUtil.log("✅ 5.2 mart_daily_summary → data_mart");

            LoggerUtil.log("=== TOÀN BỘ PIPELINE HOÀN TẤT THÀNH CÔNG! ===");
            LoggerUtil.log("→ Staging → Warehouse (SCD2) → Data Mart đã sẵn sàng cho BI");

            LoggerUtil.log("=== KẾT THÚC PIPELINE ===");

        } catch (Exception e) {
            LoggerUtil.log("❌ Lỗi: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
