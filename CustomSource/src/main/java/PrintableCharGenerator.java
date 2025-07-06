import java.security.SecureRandom;

public class PrintableCharGenerator {
    public static void main(String[] args) {
        SecureRandom secureRandom = new SecureRandom();
        // 生成32-126之间的ASCII码（可打印字符）
        int asciiCode = 32 + secureRandom.nextInt(95);
        char randomChar = (char) asciiCode;

        System.out.println("可打印字符: " + randomChar);
        System.out.println("ASCII码: " + asciiCode);
    }
}
