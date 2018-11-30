from info_summary_filter import BaseFilter

class MemoryFilter(BaseFilter):
    """base on the python set type to filter data"""

    def _get_storage(self):
        return set()

    def _save(self, hash_value):
        """
        use the python set to store the data
        :param hash_value:
        :return:
        """
        return self.storage.add(hash_value)


    def _is_exists(self, hash_value):
        if hash_value in self.storage:
            return True
        return False