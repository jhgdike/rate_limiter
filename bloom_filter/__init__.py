# coding: utf-8


class BloomFilter(object):
    """
    布隆过滤器需要：
    1. 一个位向量
    2. n个种子用来生成hash函数
    3. n个hash函数
    """

    def __init__(self):
        self.bit_size = 1 << 21
        self.bitset = BitVector(size=self.bit_size)
        self.seeds = [12, 23, 34, 45, 56, 67, 78, 89]
        self.hash_func_list = [Hash(self.bit_size, seed) for seed in self.seeds]

    def insert(self, string):
        for hash_func in self.hash_func_list:
            i = hash_func.hash(string)
            self.bitset[i] = 1
