package vn.edu.hcmuaf.fit;

import vn.edu.hcmuaf.fit.crawl.Extract;
import vn.edu.hcmuaf.fit.db.DatabaseConfig;
import vn.edu.hcmuaf.fit.load_data.LoadToDB;
import vn.edu.hcmuaf.fit.util.LoggerUtil;
import vn.edu.hcmuaf.fit.transform.LoadDimDateToWarehouse;
import vn.edu.hcmuaf.fit.transform.LoadDimProductToWarehouse;
import vn.edu.hcmuaf.fit.transform.LoadFactProductPriceDaily;
import vn.edu.hcmuaf.fit.transform.CreateAggregateDailySales;
import vn.edu.hcmuaf.fit.transform.CreateAggregateDailySummary;
import vn.edu.hcmuaf.fit.mart.LoadMartDailySales;
import vn.edu.hcmuaf.fit.mart.LoadMartDailySummary;

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
            LoggerUtil.log("=== BẮT ĐẦU PIPELINE - SCRIPT 4 & 5 ===");
            // --------------------- SCRIPT 4: WAREHOUSE ---------------------
            LoggerUtil.log("=== SCRIPT 4: LOAD DỮ LIỆU VÀO WAREHOUSE ===");

            LoadDimDateToWarehouse.load();
            LoggerUtil.log("✅ 4.1 dim_date → warehouse_db");
            LoadDimProductToWarehouse.load();
            LoggerUtil.log("✅ 4.2 dim_product (SCD Type 2) → warehouse_db");
            LoadFactProductPriceDaily.load();
            LoggerUtil.log("✅ 4.3 fact_product_price_daily → warehouse_db");
            // --------------------- SCRIPT 5: AGGREGATE & DATA MART ---------------------
            LoggerUtil.log("=== SCRIPT 5: TẠO AGGREGATE & LOAD VÀO DATA MART ===");

            CreateAggregateDailySales.load();
            LoggerUtil.log("✅ 5.1a agg_daily_sales (aggregate) → warehouse_db");
            CreateAggregateDailySummary.load();
            LoggerUtil.log("✅ 5.2a agg_daily_summary (aggregate) → warehouse_db");
            LoadMartDailySales.load();
            LoggerUtil.log("✅ 5.1b mart_daily_sales → data_mart");
            LoadMartDailySummary.load();
            LoggerUtil.log("✅ 5.2b mart_daily_summary → data_mart");
            LoggerUtil.log("=== TOÀN BỘ PIPELINE HOÀN TẤT THÀNH CÔNG! ===");
            LoggerUtil.log("→ Warehouse (SCD Type 2) → Aggregate → Data Mart đã sẵn sàng cho BI");
            LoggerUtil.log("=== KẾT THÚC PIPELINE ===");

        } catch (Exception e) {
            LoggerUtil.log("❌ Lỗi: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
