# base on mysql filter data
import pymysql
from info_summary_filter import BaseFilter
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


# class Filter(Base):
#
#     __tablename__ = 'filter'
#
#     id = Column(Integer, primary_key=True)
#     hash_value = Column(String(40), index=True, unique=True)


class MySQLFilter(BaseFilter):
    """Base on MySQL filter data"""
    def __init__(self, *args, **kwargs):

        self.table = type(
            kwargs['mysql_table_name'],
            (Base, ),
            dict(
                __tablename__=kwargs['mysql_table_name'],
                id=Column(Integer, primary_key=True),
                hash_value=Column(String(40), unique=True, index=True)
            )
        )

        BaseFilter.__init__(self, *args, **kwargs)

    def _get_storage(self):
        """
        :return: mysql connection object
        """
        engine = create_engine(self.mysql_url)
        Base.metadata.create_all(engine)   # create table， if exist，ignore
        session_obj = sessionmaker(engine)
        return session_obj

    def _save(self, hash_value):
        """
        use mysql stored the hash data
        :param hash_value:
        :return:
        """
        session = self.storage()
        filter_obj = self.table(hash_value=hash_value)
        session.add(filter_obj)
        session.commit()
        session.close()

    def _is_exists(self, hash_value):
        """
        :param hash_value:
        :return:
        """
        session = self.storage()
        ret = session.query(self.table).filter_by(hash_value=hash_value).first()
        session.close()
        print(ret)
        if ret is None:
            return False
        return True
