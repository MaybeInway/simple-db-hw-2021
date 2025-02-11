package simpledb.storage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * map + 链表实现 LRU cache
 * @param <K>
 * @param <V>
 */
public class LRUCache<K, V> {

    class DLinkedNode {
        K key;
        V value;
        DLinkedNode prev;
        DLinkedNode next;
        public DLinkedNode() {}
        public DLinkedNode(K _key, V _value) { key = _key; value = _value;}
    }

    private Map<K,DLinkedNode> cache = new ConcurrentHashMap<>();
    private int size;
    private int capacity;
    private DLinkedNode head, tail;

    public LRUCache(int capacity) {
        this.size = 0;
        this.capacity = capacity;

        // 使用伪头部和伪尾部节点
        head = new DLinkedNode();
        tail = new DLinkedNode();
        head.next = tail;
        tail.prev = head;
    }

    public Map<K, DLinkedNode> getCache() {
        return cache;
    }

    public int getSize() {
        return size;
    }

    public int getCapacity() {
        return capacity;
    }

    public DLinkedNode getHead() {
        return head;
    }

    public DLinkedNode getTail() {
        return tail;
    }

    /**
     * 根据 key 来获取元素
     * @param key
     * @return
     */
    public synchronized V get(K key) {
        DLinkedNode node = cache.get(key);
        if (node == null) {
            return null;
        }
        // 如果存在，移到头部
        moveToHead(node);
        return node.value;
    }

    /**
     * 插入元素，需要注意容量限制
     * @param key
     * @param value
     */
    public synchronized void put(K key, V value) {
        DLinkedNode node = cache.get(key);
        if (node == null) {
            // 新增
            DLinkedNode newNode = new DLinkedNode(key, value);
            cache.put(key, newNode);
            addToHead(newNode); // 插入到头部
            size++;
            if (size > capacity) {
                // 超出容量，移除尾部节点
                // 1. 先在链表中移除
                DLinkedNode tmp = tail.prev;
                removeNode(tmp);
                // 2. 再从 map 中移除
                cache.remove(tmp.key);
                size--;
            }
        } else {
            node.value = value;
            moveToHead(node);
        }
    }

    /**
     * 将节点移动到头部
     * @param node
     */
    public void moveToHead(DLinkedNode node) {
        removeNode(node);
        addToHead(node);
    }

    /**
     * 删除节点
     * @param node
     */
    public void removeNode(DLinkedNode node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }

    public void remove(DLinkedNode node) {
        removeNode(node);
        cache.remove(node.key);
        size--;
    }

    public void addToHead(DLinkedNode node) {
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
    }

    public synchronized void discard() {
        // 如果超出容量，删除双向链表的尾部节点
        DLinkedNode tail = removeTail();
        // 删除哈希表中的对应项
        cache.remove(tail.key);
        size--;
    }

    public DLinkedNode removeTail() {
        DLinkedNode res = tail.prev;
        removeNode(res);
        return res;
    }

}
