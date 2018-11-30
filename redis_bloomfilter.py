# -*- coding: utf-8 -*-
import redis
import hashlib

import six


class GetHashValues(object):
    """generate hash values"""

    def __init__(self, salts=None, hash_func_name='md5'):
        self.salts = salts
        if self.salts is None:
            raise Exception('please provide the salt list')
        self.hash_func = getattr(hashlib, hash_func_name)

    @staticmethod
    def _safe_data(data):
        """
        transfer the data type to binary
        :param data:  raw data
        :return: binary data
        """
        if six.PY3:
            if isinstance(data, bytes):
                return data
            elif isinstance(data, str):
                return data.encode()
            else:
                raise Exception('please provide the string data')
        else:
            if isinstance(data, str):
                return data
            elif isinstance(data, unicode):
                return data.encode()
            else:
                raise Exception('please provide the string data')

    def get_hash_values(self, data):
        """
        generate the multiple hash value
        :param data:  data need to hash
        :return: hash_value_list
        """
        if type(self.salts) != list:
            raise Exception('salts must be list')
        hash_value_list = []
        for salt in self.salts:
            hash_obj = self.hash_func()
            hash_obj.update(self._safe_data(data))
            hash_obj.update(self._safe_data(salt))
            ret = hash_obj.hexdigest()
            hash_value_list.append(ret)
        return hash_value_list


class BloomFilter(object):

    def __init__(self, host="localhost", port=6379, db=0, salts=None, redis_key="bloomfilter"):
        self.host = host
        self.port = port
        self.db = db
        self.redis_key = redis_key
        self.client = self.get_redis_client()
        self.hash_value = GetHashValues(salts)

    def get_redis_client(self):
        """
        create redis connection pool
        :return: redis client
        """
        connection_pool = redis.ConnectionPool(host=self.host, port=self.port, db=self.db)
        redis_client = redis.StrictRedis(connection_pool=connection_pool)
        return redis_client

    def _get_offset_list(self, data):
        hash_value_list = self.hash_value.get_hash_values( data )
        offset_value_list = []
        for hash_value in hash_value_list:
            offset_value = self._get_offset(hash_value)
            offset_value_list.append(offset_value)
        return offset_value_list

    def save_data(self, data):
        """
        save data to redis
        :param data:  raw data
        :return: True or False
        """
        offset_list = self._get_offset_list(data)
        for offset_value in offset_list:
            self.client.setbit(self.redis_key, offset_value, 1)
        return True

    @staticmethod
    def _get_offset(hash_value):
        """
        2**9 = 256  means 256MB length bit array
        2**20 = 1024 * 1024
        :param hash_value:
        :return:
        """
        return int(hash_value, 16) % (2**9 * 2**20 * 2**3)

    def is_exists(self, data):
        """
        judge the bit location value
        :param data:
        :return: True or False
        """
        offset_list = self._get_offset_list(data)
        ret_list = []
        for offset_value in offset_list:
            ret = self.client.getbit(self.redis_key, offset_value)
            ret_list.append(ret)
            if ret == 0:
                return False
        return True


if __name__ == '__main__':
    bl = BloomFilter(salts=['a', 'b', 'c'], db=15)
    # test_data = ['13211', '223212', '331233', 'aa32a', 'bb412b', '11123121', '33123233', 'c32132cc']
    test_data = ['111', '111', '222', '333', '111', 'aaa', '333', 'aaa', 'bbb', 'bb412b']
    for data in test_data:
        if not bl.is_exists(data):
            bl.save_data( data )
            print('data saved: ', data)
        else:
            print("data existed: ", data)



