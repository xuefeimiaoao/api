package algorithm;

import java.util.ArrayDeque;
import java.util.Stack;

public class MergeTwoList {
    public static void main(String[] args) {
        MergeTwoList executor = new MergeTwoList();
//        executor.mergeTwoLists();
    }
    public ListNode mergeTwoLists(ListNode list1, ListNode list2) {
        if (list1 == null) {
            return list2;
        }
        if (list2 == null) {
            return list1;
        }


        ListNode currentNode = null;
        ListNode anotherNode = null;
        ListNode head = null;
        if (list1.val <= list2.val) {
            currentNode = list1;
            anotherNode = list2;
        } else {
            currentNode = list2;
            anotherNode = list1;
        }
        head = currentNode;


        while (currentNode != null && currentNode.next != null) {
            // 1224
            // 135
            if (currentNode.next != null && currentNode.next.val <= anotherNode.val) {
                currentNode = currentNode.next;
                continue;
            }
            if (currentNode.next == null && anotherNode != null) {
                currentNode.next = anotherNode;
                continue;
            }

            //another 2; current 1
            // 1224 another  current(2) current(2)  another(4) current(4)
            // 135  current  another(3)             current(3) another(5) current(5)

            //[1,2,4]
            //[1,3,4]
            if (anotherNode != null && currentNode != null && currentNode.val <= anotherNode.val) {
                ListNode tmp = currentNode.next;
                currentNode.next = anotherNode;
                currentNode = anotherNode;
                anotherNode = tmp;
                continue;
            }

            if (currentNode.next == null && anotherNode.next == null) {
                break;
            }
            if (anotherNode == null) {
                break;
            }

        }

        return head;
    }
}

class ListNode {
    int val;
    ListNode next;
    ListNode() {}
    ListNode(int val) { this.val = val; }
    ListNode(int val, ListNode next) { this.val = val; this.next = next; }
}
