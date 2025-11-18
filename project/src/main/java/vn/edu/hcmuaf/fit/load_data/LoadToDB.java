package vn.edu.hcmuaf.fit.load_data;

import com.opencsv.CSVReader;
import vn.edu.hcmuaf.fit.db.DatabaseConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LoadToDB {
    public static void load(String csvFile) throws Exception {
        String sql = """
            INSERT INTO stg_products_raw (product_name, brand, price, original_price, url, image_url)
            VALUES (?, ?, ?, ?, ?, ?)
        """;
        //2. Kết nối đến DB
        try (Connection conn = DatabaseConfig.getConnection()) {
            //3. Đọc file csv
            CSVReader reader = new CSVReader(new FileReader(csvFile));
            reader.readNext(); // bỏ header
            //4. Khởi tạo lệnh insert với Batch Processing
            PreparedStatement ps = conn.prepareStatement(sql);
            String[] nextLine;
            int count = 0;
            //5. Đọc từng dòng csv và thêm vào batch
            while ((nextLine = reader.readNext()) != null) {
                ps.setString(1, nextLine[0]);
                ps.setString(2, nextLine[1]);
                ps.setString(3, nextLine[2]);
                ps.setString(4, nextLine[3]);
                ps.setString(5, nextLine[4]);
                ps.setString(6, nextLine[5]);
                ps.addBatch();
                //6. Thực thi batch
                if (++count % 100 == 0) {
                    ps.executeBatch();
                    LoggerUtil.log("Đã import " + count + " dòng...");
                }
            }
            //6. Thực thi batch
            ps.executeBatch();
            LoggerUtil.log("Import hoàn tất, tổng: " + count + " bản ghi.");
        } //2.1 Đóng kết nối nếu lỗi
    }

    public static void main(String[] args) {
        System.out.println("[START] Import process started...");
        try {
            //1.1 Tạo bảng "stg_products_raw"
            DatabaseConfig.createTableIfNotExists();

            //3. Đọc file csv
            String dateStr = new SimpleDateFormat("dd_MM_yy").format(new Date());
            String csvFile = System.getProperty("user.dir") + "/" + dateStr + "_products.csv";

            System.out.println("[INFO] Loading file: " + csvFile);
            load(csvFile);

            System.out.println("[DONE] Import completed successfully.");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("[ERROR] Import failed: " + e.getMessage());
        }
    }

}


