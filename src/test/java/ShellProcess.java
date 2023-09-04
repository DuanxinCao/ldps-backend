import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ShellProcess {
    public static void main(String[] args) {
        try {
            // 构建命令行
            String command = "sudo /etc/init.d/clickhouse-server start"; // 替换为您要执行的Shell命令
            ProcessBuilder processBuilder = new ProcessBuilder();
            processBuilder.command("bash", "-c", command);

            // 启动进程
            Process process = processBuilder.start();

            // 读取输出
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }

            // 等待进程执行完毕
            int exitCode = process.waitFor();
            System.out.println("Exit code: " + exitCode);

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
