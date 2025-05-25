import os
import pickle
from indices.base_index import BaseIndex

ORDER = 4

class BTreeNode:
    def __init__(self, is_leaf=True):
        self.keys = []  # lista de tuplas (key, value)
        self.children = []
        self.is_leaf = is_leaf
        self.next = None

class BPlusTree(BaseIndex):
    def __init__(self, path='btree_index.pkl'):
        self.path = path
        if os.path.exists(self.path):
            with open(self.path, 'rb') as f:
                self.root = pickle.load(f)
        else:
            self.root = BTreeNode()
            self._save()

    def _save(self):
        with open(self.path, 'wb') as f:
            pickle.dump(self.root, f)

    def search(self, key):
        node = self.root
        while not node.is_leaf:
            i = 0
            while i < len(node.keys) and key > node.keys[i][0]:
                i += 1
            node = node.children[i]
        return [v for k, v in node.keys if k == key]

    def range_search(self, start_key, end_key):
        result = []
        node = self.root
        while not node.is_leaf:
            i = 0
            while i < len(node.keys) and start_key > node.keys[i][0]:
                i += 1
            node = node.children[i]
        while node:
            for k, v in node.keys:
                if start_key <= k <= end_key:
                    result.append((k, v))
                elif k > end_key:
                    return result
            node = node.next
        return result

    def insert(self, key, values):
        root = self.root
        if len(root.keys) == ORDER - 1:
            new_root = BTreeNode(is_leaf=False)
            new_root.children.append(root)
            self._split_child(new_root, 0)
            self.root = new_root
        self._insert_non_full(self.root, key, values)
        self._save()

    def _insert_non_full(self, node, key, value):
        if node.is_leaf:
            node.keys.append((key, value))
            node.keys.sort()
        else:
            i = len(node.keys) - 1
            while i >= 0 and key < node.keys[i][0]:
                i -= 1
            i += 1
            if len(node.children[i].keys) == ORDER - 1:
                self._split_child(node, i)
                if key > node.keys[i][0]:
                    i += 1
            self._insert_non_full(node.children[i], key, value)

    def _split_child(self, parent, i):
        node = parent.children[i]
        mid = len(node.keys) // 2

        if node.is_leaf:
            right = BTreeNode()
            right.keys = node.keys[mid:]
            node.keys = node.keys[:mid]
            right.next = node.next
            node.next = right
        else:
            right = BTreeNode(is_leaf=False)
            right.keys = node.keys[mid+1:]
            right.children = node.children[mid+1:]
            node.keys = node.keys[:mid]
            node.children = node.children[:mid+1]

        parent.keys.insert(i, right.keys[0])
        parent.children.insert(i+1, right)

    def remove(self, key):
        node = self.root
        while not node.is_leaf:
            i = 0
            while i < len(node.keys) and key > node.keys[i][0]:
                i += 1
            node = node.children[i]
        node.keys = [(k, v) for (k, v) in node.keys if k != key]
        self._save()

    def scan_all(self):
        node = self.root
        while not node.is_leaf:
            node = node.children[0]
        result = []
        while node:
            for k, v in node.keys:
                result.append(f"{k} -> {v}")
            node = node.next
        return result
