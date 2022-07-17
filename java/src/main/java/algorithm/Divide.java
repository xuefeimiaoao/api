package algorithm;

import java.util.ArrayList;


public class Divide {
    public static void main(String[] args) {
        Divide divide = new Divide();
//        System.out.println("7/-3=: " + divide.divide(7, -3) + "; expected: -2");
//        System.out.println("10/3=: " + divide.divide(10, 3) + "; expected: 3");
//        System.out.println("1/1=: " + divide.divide(1, 1) + "; expected: 1");
//        System.out.println("-2147483648/-1=: " + divide.divide(-2147483648, -1) + "; expected: 2147483647");
//        System.out.println("2147483647/1=: " + divide.divide(2147483647, 1) + "; expected: 2147483647");
//        System.out.println("-2147483648/1=: " + divide.divide(-2147483648, 1) + "; expected: -2147483648");
        System.out.println("-2147483648/2=: " + divide.divide(-2147483648, 2) + "; expected: -1073741824");

    }
    public int divide(int dividend, int divisor) {

        if (divisor == Integer.MIN_VALUE) {
            if (dividend == Integer.MIN_VALUE) {
                return 1;
            } else {
                return 0;
            }
        }
        if (dividend == Integer.MIN_VALUE) {
            if (divisor == -1) {
                return Integer.MAX_VALUE;
            } else if (divisor == 1) {
                return Integer.MIN_VALUE;
            }
            // todo 有问题！想一个能够直接处理负数的方法。
            int[] calc = calc(Math.abs(dividend + 1), Math.abs(divisor));
            int ret = calc[1] + 1 >= Math.abs(divisor) && calc[1] != 0 ? calc[0] + 1 : calc[0];
            return (dividend ^ divisor) < 0 ? -ret : ret;
        }

        int calc = calc(Math.abs(dividend), Math.abs(divisor))[0];
        // 异或计算，判断是否异号
        return (dividend ^ divisor) < 0 ? -calc : calc;
    }

    public int[] calc(int dividend, int divisor) {
        // positive
        // current 3 6 12 24 48
        // index   0 1  2
        int current = divisor;
        int index = 0;
        int ret = 0;

        if (current == dividend) {
            return new int[]{1, 0};
        }

        // move forward
        // notice that current can be overhead!!!
        while (current < dividend && index <= 32 - getOccupyingBitsOfPostiveInt(divisor)) {
            ret = 1 << index;
            // move forward
            index++;
            current = divisor << index;
            if (current == dividend) {
                ret = 1 << index;
                return new int[]{ret, 0};
            }
        }

        // move backward
        index--;
        current = (divisor << index);
        index--;
        while (current < dividend && index >= 0) {
            int tmpSum = current + (divisor << index);
            // todo 这边的tmpSum可能会超过32位限制
            if (tmpSum > dividend) {
                index--;
            } else if (tmpSum == dividend) {
                ret += 1 << index;
                return new int[]{ret, 0};
            } else {
                ret += 1 << index;
                current += tmpSum;
                index--;
            }
        }

        return new int[]{ret, dividend - current};
    }

    public int getOccupyingBitsOfPostiveInt(int target) {
        // calc the digit of divisor
        // 101
        int m = Integer.MAX_VALUE;
        int bit = 0;
        int tmp = target;
        if (target == 0) {
            return bit;
        }
        while ((tmp & m) != 0) {
            bit++;
            tmp = tmp >> bit;
        }
        // then ,bit + 1 is the count of bits
        return bit + 1;
    }
}