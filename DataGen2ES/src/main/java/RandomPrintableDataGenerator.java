import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Random;

public class RandomPrintableDataGenerator {
    // 可打印字符集（ASCII 33~126范围，排除空格和控制字符）
    private static final String PRINTABLE_CHARS = generatePrintableCharSet();

    private static String generatePrintableCharSet() {
        StringBuilder sb = new StringBuilder();
        for (int i = 33; i <= 126; i++) { // ASCII可打印字符范围
            char c = (char) i;
            if (c != ' ') { // 排除空格（按需调整）
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * 生成指定字节大小的可打印字符串
     * @param targetBytes 目标字节大小
     * @param useSecureRandom 是否使用安全随机数生成器
     * @return 符合字节大小的随机字符串
     */
    public static String generate(int targetBytes, boolean useSecureRandom) {
        if (targetBytes <= 0) throw new IllegalArgumentException("目标字节大小必须为正整数");

        Random random = useSecureRandom ? new SecureRandom() : new Random();
        StringBuilder sb = new StringBuilder();
        int currentBytes = 0;

        // 动态生成字符直到达到目标字节大小
        while (currentBytes < targetBytes) {
            char randomChar = PRINTABLE_CHARS.charAt(random.nextInt(PRINTABLE_CHARS.length()));
            String charStr = String.valueOf(randomChar);
            int charBytes = charStr.getBytes(StandardCharsets.UTF_8).length;

            // 检查添加后是否超出目标
            if (currentBytes + charBytes <= targetBytes) {
                sb.append(randomChar);
                currentBytes += charBytes;
            }
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        // 示例：生成100字节的可打印数据（使用安全随机数）
        String data = generate(100, true);
        System.out.println("生成结果: " + data);
        System.out.println("实际字节数: " + data.getBytes(StandardCharsets.UTF_8).length);
    }
}