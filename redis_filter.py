# stored the data into redis and use redis zset filter data
import redis
from . import BaseFilter

class RedisFilter(BaseFilter):
    """base redis Persistent storage"""

    def _get_storage(self):
        """
        :return:redis connection object
        """
        pool = redis.ConnectionPool(host=self.redis_host, port=self.redis_port, db=self.redis_db, max_connections=1024)
        client = redis.StrictRedis(connection_pool=pool)
        return client

    def _save(self, hash_value):
        """
        use redis set stored
        :param hash_value:
        :return:
        """
        return self.storage.sadd(self.redis_key, hash_value)

    def _is_exists(self, hash_value):
        """
        judge the hash_value exists or not
        :param hash_value:
        :return:
        """
        return self.storage.sismember(self.redis_key, hash_value)