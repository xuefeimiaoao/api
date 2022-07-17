package algorithm;

public class ZConvert {
    public static void main(String[] args) {
        ZConvert executor = new ZConvert();
        String s = "PAYPALISHIRING";
        int numRows = 4;
        System.out.println(executor.convert(s, numRows).equals("PINALSIGYAHRPI"));

    }

    //P     I    N          00          04
    //A   L S  I G          10      13
    //Y A   H R             20  21
    //P     I               30
    public String convert(String s, int numRows) {
        char[] input = s.toCharArray();
        // 最多需要numRows列
        char[][] output = new char[numRows][input.length];
        char[] converted = new char[input.length];
        int rowPointer = 0;
        int rankPointer = 0;
        boolean shouldZ = false;
        for (int i = 0; i < input.length; i++) {
            // 注意两个指针的衔接部分。
            if (!shouldZ) {
                // 一直向下
                output[rowPointer][rankPointer] = input[i];
                if (rowPointer >= numRows - 1) {
                    shouldZ = true;
                    rowPointer--;
                }
                rowPointer++;
            } else {
                // 转成Z字
                rankPointer++;
                rowPointer--;
                output[rowPointer][rankPointer] = input[i];
                if (rowPointer == 0) {
                    shouldZ = false;
                    rowPointer++;
                }
            }
        }

        // read
        // 注意读取的顺序
        int index = 0;
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j <= rankPointer; j++) {
                if (output[i][j] != 0) {
                    converted[index++] = output[i][j];
                }
            }
        }
        return new String(converted);
    }
}
