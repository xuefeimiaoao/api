package algorithm;

public class IntReverse {
    public static void main(String[] args) {
        //给你一个 32 位的有符号整数 x ，返回将 x 中的数字部分反转后的结果。
        //
        //如果反转后整数超过 32 位的有符号整数的范围[−2^31, 2^31− 1] ，就返回 0。
        //
        //假设环境不允许存储 64 位整数（有符号或无符号）。
        //
        //来源：力扣（LeetCode）
        //链接：https://leetcode.cn/problems/reverse-integer
        //著作权归领扣网络所有。商业转载请联系官方授权，非商业转载请注明出处。

        //输入：x = -123
        //输出：-321

        IntReverse executor = new IntReverse();
        int x = -123;
        int v = executor.reverse(x);
        System.out.println("反转后的数值：" + v);
        System.out.println(v == -321);
    }

    public int reverse(int x) {
        if (x == 0) {
            return 0;
        } else {
            try {
                return reversePositive(x, x > 0);
            } catch (Exception e) {
                return 0;
            }
        }
    }

    public int reversePositive(int x, boolean isPositive) {
        int v = isPositive ? x : -x;
        char[] input = (v + "").toCharArray();
        // 124835
        // 538421

        // 12483
        // 38421

        int leftIndex = 0;
        int rightIndex = input.length - 1;
        for (int i = 0; i < input.length; i++) {
            if (leftIndex < rightIndex) {
                change(input, leftIndex++, rightIndex--);
            } else {
                // 注意，此时应该break，否则相当于白干
                break;
            }
        }

        // 翻转后0187
        if (isPositive) {
            return Integer.parseInt(new String(input));
        } else {
            // 注意，Integer.parseInt(new String("-" + input))会报错
            return Integer.parseInt("-" + new String(input));
        }
    }

    public void change(char[] input, int left, int right) {
        char tmp = input[left];
        input[left] = input[right];
        input[right] = tmp;
    }
}
