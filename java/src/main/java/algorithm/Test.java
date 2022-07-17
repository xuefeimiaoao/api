//package algorithm;
//
//import com.google.common.collect.Lists;
//
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//import java.util.Stack;
//import java.util.stream.Collectors;
//
//public class Test {
//    public static void main(String[] args) {
//        int[] n = new int[10];
//        List<Integer> ints = Lists.asList(120, n);
//
//        ArrayList<List<Integer>> integers = new ArrayList<>();
//
//        integers.stream().flatMap(l -> {
//            return l.stream().map(t -> t);
//        }).collect(Collectors.summingInt());
//        integers.remove();
//
//        integers.ad
//
//                ;
//
//        integers.stream().distinct().count()
//
//        int size = integers.size();
//        integers.get(size - 1);
//
//
//
//    }
//
//    public int longestValidParentheses(String s) {
//        char[] input = s.toCharArray();
//        Stack<Character> stack = new Stack<>();
//        int count = 0;
//
//        for (int i = 0; i < input.length; i++) {
//            if (stack.isEmpty()) {
//                stack.push(input[i]);
//            } else {
//                Character top = stack.pop();
//                if (top == '(' && input[i] == ')') {
//                    count += 2;
//                } else {
//                    stack.push('(');
//                    stack.push(')');
//                }
//            }
//        }
//        return count;
//    }
//}
