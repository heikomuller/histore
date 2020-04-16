class Histore(object):
    """History of a database is stored in three files: data.json, schema.json,
    and versions.json.
    """
    def __init__(self, repodir):
        """
        """
        pass

    def checkout(self, snapshot):
        """
        """
        pass

    def commit((self, data):
        """
        """
        pass

    def snapshots(self):
        """
        """
        pass


class Snapshot(object):
    def __init__(self, identifier, timestamp, archive):
        """
        """
        self.identifier = identifier
        self.timestamp = timestamp
        self.archive = archive


    def data(self):
        """
        """
        self.archive.checkout(self)
