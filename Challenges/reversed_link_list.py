class ListNode:
    def __init__(self, value):
        self.value = value
        self.next = None

def reverse_linked_list(head):
    prev = None
    current = head
    while current:
        next_node = current.next
        current.next = prev
        prev = current
        current = next_node
    return prev

# Example usage:
head = ListNode(1)
head.next = ListNode(2)
head.next.next = ListNode(3)

reversed_head = reverse_linked_list(head)
while reversed_head:
    print(reversed_head.value)
    reversed_head = reversed_head.next