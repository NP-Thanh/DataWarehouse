package vn.edu.hcmuaf.fit.load_data;

import com.opencsv.CSVReader;
import vn.edu.hcmuaf.fit.db.ControlDBConfig;
import vn.edu.hcmuaf.fit.db.DatabaseConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LoadToDB {
    public static int load(String csvFile) throws Exception {
        String sql = """
            INSERT INTO stg_products_raw (product_name, brand, price, original_price, url, image_url)
            VALUES (?, ?, ?, ?, ?, ?)
        """;
        int count = 0;

        //2. Kết nối đến DB
        try (Connection conn = DatabaseConfig.getConnection()) {
            //3. Đọc file csv
            CSVReader reader = new CSVReader(new FileReader(csvFile));
            reader.readNext(); // bỏ header
            //4. Khởi tạo lệnh insert với Batch Processing
            PreparedStatement ps = conn.prepareStatement(sql);
            String[] nextLine;
            //5. Đọc từng dòng csv và thêm vào batch
            while ((nextLine = reader.readNext()) != null) {
                ps.setString(1, nextLine[0]);
                ps.setString(2, nextLine[1]);
                ps.setString(3, nextLine[2]);
                ps.setString(4, nextLine[3]);
                ps.setString(5, nextLine[4]);
                ps.setString(6, nextLine[5]);
                ps.addBatch();
                count++;
            }
            //6. Thực thi batch
            ps.executeBatch();
            LoggerUtil.log("Import hoàn tất, tổng: " + count + " bản ghi.");
        } //2.1 Đóng kết nối nếu lỗi
        return count;
    }

    public static void main(String[] args) {
        String runId = null;
        int recordCount = 0;
        String dateStr = new SimpleDateFormat("dd_MM_yy").format(new Date());

        try {
            // 1. GHI LOG BẮT ĐẦU VÀO CONTROL DB
            runId = LoggerUtil.startProcess(LoggerUtil.SOURCE_CELLPHONES_ID, "Load to staging");
            if (runId == null) {
                throw new Exception("Không thể khởi tạo Run ID hoặc kết nối Control DB bị lỗi.");
            }
            LoggerUtil.log("Bắt đầu thực thi Script 2: LOAD TO STAGING.");

            // 2. KHỞI TẠO BẢNG ĐÍCH (Staging DB)
            DatabaseConfig.createTableIfNotExists();

            // 3. XÁC ĐỊNH FILE CẦN LOAD
            String csvFileName = dateStr + "_products.csv";
            String csvFile = csvFileName;

            File file = new File(csvFile);
            if (!file.exists()) {
                throw new Exception("File CSV không tồn tại tại đường dẫn: " + csvFile + ". Vui lòng chạy Script 1 (Extract) trước.");
            }

            LoggerUtil.log("Đang nạp file: " + csvFile);

            // 4. THỰC HIỆN LOAD DỮ LIỆU
            recordCount = load(csvFile);

            // 5. KẾT THÚC THÀNH CÔNG VÀ CẬP NHẬT CONTROL DB
            LoggerUtil.endProcess(recordCount, "SUCCESS", null);
            LoggerUtil.log("Load To Staging hoàn tất, tổng bản ghi: " + recordCount);

            // 6. XUẤT FILE CONFIG (lưu thông tin trạng thái)
            LoggerUtil.exportConfigFile(dateStr);

        } catch (Exception e) {
            // 7. KẾT THÚC THẤT BẠI VÀ GHI LỖI VÀO CONTROL DB
            if (runId != null) {
                LoggerUtil.endProcess(recordCount, "FAILED", "Lỗi Load To Staging: " + e.getMessage());
            }
            System.err.println("[ERROR] Load To Staging failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

}


