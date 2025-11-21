package vn.edu.hcmuaf.fit.crawl;

import com.opencsv.CSVWriter;
import io.github.bonigarcia.wdm.WebDriverManager;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class Extract {
    // URL danh mục điện thoại
    private static final String TARGET_URL = "https://cellphones.com.vn/mobile.html";

    public static String crawlToCSV() throws IOException {
        String dateStr = new SimpleDateFormat("dd_MM_yy").format(new Date());
        String csvFile = dateStr + "_products.csv";

        // 1. Cấu hình Selenium
        WebDriverManager.chromedriver().setup();
        ChromeOptions options = new ChromeOptions();
        options.addArguments("--headless"); // chạy ẩn (không hiện Chrome lên)
        options.addArguments("--disable-notifications"); // Tắt thông báo
        options.addArguments("--start-maximized"); // Mở full màn hình

        WebDriver driver = new ChromeDriver(options);

        try (CSVWriter writer = new CSVWriter(new FileWriter(csvFile))) {
            writer.writeNext(new String[]{"product_name", "brand", "price", "original_price", "url", "image_url"});

            LoggerUtil.log("=== Bắt đầu mở trình duyệt: " + TARGET_URL);
            driver.get(TARGET_URL);

            // Chờ trang load lần đầu
            Thread.sleep(3000);

            // 2. Logic Tự động Click "Xem thêm"
            // Chúng ta sẽ click cho đến khi không còn nút đó hoặc đạt giới hạn mong muốn
            int clickCount = 0;
            int maxClicks = 10; // Ví dụ: Giới hạn click 10 lần (tầm 200+ sản phẩm). Muốn lấy hết thì tăng số này lên.

            while (clickCount < maxClicks) {
                try {
                    // Tìm nút "Xem thêm".
                    List<WebElement> buttons = driver.findElements(By.cssSelector(".btn-show-more"));

                    if (buttons.isEmpty() || !buttons.get(0).isDisplayed()) {
                        LoggerUtil.log("Không thể load dữ liệu. Kết thúc process Extract!");
                        break;
                    }

                    WebElement showMoreBtn = buttons.get(0);

                    // Scroll xuống nút đó để chắc chắn click được
                    ((JavascriptExecutor) driver).executeScript("arguments[0].scrollIntoView(true);", showMoreBtn);
                    Thread.sleep(1000); // Nghỉ xíu sau khi scroll

                    // Click bằng Javascript
                    ((JavascriptExecutor) driver).executeScript("arguments[0].click();", showMoreBtn);

                    clickCount++;
                    LoggerUtil.log("Đã click 'Xem thêm' lần: " + clickCount);

                    // Chờ dữ liệu mới load lên (Quan trọng)
                    Thread.sleep(2000);

                } catch (Exception e) {
                    LoggerUtil.log("Có lỗi khi click hoặc đã hết trang: " + e.getMessage());
                    break;
                }
            }

            // 3. Lấy toàn bộ HTML sau khi đã load xong
            LoggerUtil.log("Đang trích xuất dữ liệu HTML...");
            String pageSource = driver.getPageSource();
            Document doc = Jsoup.parse(pageSource);

            // 4. Parse dữ liệu bằng Jsoup (Logic cũ của bạn)
            Elements products = doc.select("div.product-item");
            LoggerUtil.log("Tìm thấy tổng cộng: " + products.size() + " sản phẩm.");

            for (Element p : products) {
                try {
                    String name = p.select("div.product__name").text();
                    String brand = detectBrand(name);

                    // Xử lý giá
                    String price = p.select("p.product__price--show").text();
                    if(price.isEmpty()) price = "0";

                    String discount = p.select("p.product__price--through").text();

                    // Xử lý Link
                    String linkAttr = p.select("a").attr("href");
                    String link = linkAttr.startsWith("http") ? linkAttr : "https://cellphones.com.vn" + linkAttr;

                    // Xử lý ảnh
                    String img = p.select("img").attr("src");

                    if (!name.isEmpty()) {
                        writer.writeNext(new String[]{name, brand, price, discount, link, img});
                    }
                } catch (Exception ex) {
                    continue;
                }
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            // Đóng trình duyệt
            driver.quit();
            LoggerUtil.log("Trình duyệt đã load xong.");
        }
        return csvFile;
    }

    private static String detectBrand(String name) {
        String lower = name.toLowerCase();
        if (lower.contains("iphone") || lower.contains("apple")) return "Apple";
        if (lower.contains("samsung")) return "Samsung";
        if (lower.contains("xiaomi")) return "Xiaomi";
        if (lower.contains("oppo")) return "Oppo";
        if (lower.contains("vivo")) return "Vivo";
        if (lower.contains("realme")) return "Realme";
        if (lower.contains("asus")) return "Asus";
        if (lower.contains("nokia")) return "Nokia";
        if (lower.contains("sony")) return "Sony";
        if (lower.contains("tecno")) return "Tecno";
        if (lower.contains("infinix")) return "Infinix";
        return "Other";
   }

    public static void main(String[] args) {

        String runId = null;
        int recordCount = 0;
        String dateStr = new SimpleDateFormat("dd_MM_yy").format(new Date());

        try {
            // 1. GHI LOG BẮT ĐẦU VÀO CONTROL DB
            // Sử dụng ID nguồn Cellphones (1) và tên Operator (ETL_JOB)
            runId = LoggerUtil.startProcess(LoggerUtil.SOURCE_CELLPHONES_ID, "Extract");
            if (runId == null) {
                throw new Exception("Không thể khởi tạo Run ID hoặc kết nối Control DB bị lỗi.");
            }

            // 2. CHẠY LOGIC CÀO DỮ LIỆU
            LoggerUtil.log("Bắt đầu thực thi Script 1: EXTRACT.");
            String csvFile = crawlToCSV();

            // Đọc lại số lượng bản ghi (Tuy nhiên, do hàm crawlToCSV hiện không trả về count, ta cần ước tính)
            recordCount = (int) java.nio.file.Files.lines(new File(csvFile).toPath()).count() - 1;

            // 3. KẾT THÚC THÀNH CÔNG VÀ CẬP NHẬT CONTROL DB
            LoggerUtil.endProcess(recordCount, "SUCCESS", null);
            LoggerUtil.log("✅ Extract hoàn tất, tổng bản ghi: " + recordCount);

            // 4. XUẤT FILE CONFIG (lưu thông tin trạng thái)
            LoggerUtil.exportConfigFile(dateStr);

        } catch (Exception e) {
            LoggerUtil.endProcess(recordCount, "FAILED", "Lỗi Extract: " + e.getMessage());
            System.err.println("[ERROR] Crawl failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}