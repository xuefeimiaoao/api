package algorithm;

import java.util.ArrayDeque;

public class Some {
    public static void main(String[] args) {
        Some some = new Some();
        //System.out.println(some.isValid("([)]"));
        System.out.println(some.isValid("{[]}"));
    }
    public boolean isValid(String s) {
        ArrayDeque<Character> deque = new ArrayDeque();
        char[] chars = s.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            if (deque.isEmpty()) {
                deque.offer(chars[i]);
                continue;
            }

            char top = deque.getLast();
            char cur = chars[i];

            if (top == '(' && cur == ')'
                    || top == '[' && cur == ']' || top == '{' && cur == '}') {
                deque.removeLast();
            } else {
                deque.offer(chars[i]);
            }
        }

        return deque.isEmpty();

    }
}
