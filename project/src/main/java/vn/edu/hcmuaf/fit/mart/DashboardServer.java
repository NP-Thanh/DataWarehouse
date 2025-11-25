package vn.edu.hcmuaf.fit.mart;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DashboardServer {

    public static void startServer() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
            server.createContext("/", new HttpHandler() {
                @Override
                public void handle(HttpExchange exchange) throws IOException {
                    String path = exchange.getRequestURI().getPath();
                    if (path.equals("/") || path.equals("/index.html")) {
                        path = "/dashboard/ui_mart.html";
                    }
                    File file = new File("." + path).getCanonicalFile();

                    if (!file.getPath().startsWith(new File(".").getCanonicalPath())) {
                        sendResponse(exchange, 403, "Forbidden");
                        return;
                    }

                    if (file.exists() && !file.isDirectory()) {
                        String content = Files.readString(Paths.get(file.getPath()));
                        String mime = "text/html";
                        if (path.endsWith(".css")) mime = "text/css";
                        if (path.endsWith(".js")) mime = "application/javascript";

                        exchange.getResponseHeaders().set("Content-Type", mime + "; charset=utf-8");
                        exchange.sendResponseHeaders(200, content.getBytes("UTF-8").length);
                        OutputStream os = exchange.getResponseBody();
                        os.write(content.getBytes("UTF-8"));
                        os.close();
                    } else {
                        sendResponse(exchange, 404, "Not Found");
                    }
                }
            });
            server.setExecutor(null);
            server.start();
            System.out.println("");
            System.out.println("DASHBOARD ĐÃ CHẠY TRÊN LOCALHOST");
            System.out.println("MỞ TRÌNH DUYỆT → GÕ: http://localhost:8080");
            System.out.println("THẦY CHỈ CẦN MỞ LINK NÀY LÀ THẤY GIAO DIỆN ĐẸP NHƯ POWER BI");
            System.out.println("");

            // Tự động mở trình duyệt (Windows + Ubuntu đều chạy ngon)
            String url = "http://localhost:8080";
            String os = System.getProperty("os.name").toLowerCase();
            if (os.contains("win")) {
                new ProcessBuilder("cmd", "/c", "start", "", url).start();
            } else if (os.contains("linux")) {
                new ProcessBuilder("xdg-open", url).start();
            } else if (os.contains("mac")) {
                new ProcessBuilder("open", url).start();
            }

        } catch (Exception e) {
            System.out.println("Lỗi khởi động server: " + e.getMessage());
        }
    }

    private static void sendResponse(HttpExchange exchange, int code, String response) throws IOException {
        exchange.sendResponseHeaders(code, response.getBytes().length);
        OutputStream os = exchange.getResponseBody();
        os.write(response.getBytes());
        os.close();
    }
}