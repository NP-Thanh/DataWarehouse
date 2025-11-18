package vn.edu.hcmuaf.fit;

import vn.edu.hcmuaf.fit.crawl.Extract;
import vn.edu.hcmuaf.fit.db.DatabaseConfig;
import vn.edu.hcmuaf.fit.load_data.LoadToDB;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

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

            LoggerUtil.log("=== KẾT THÚC PIPELINE ===");
        } catch (Exception e) {
            LoggerUtil.log("❌ Lỗi: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
