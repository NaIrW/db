import pymongo


class DBE:

    def __init__(self, db, eid):
        self.db = db
        self.eid = eid

    def update(self, data):
        self.db.update(self.eid, data)

    def replace(self, data):
        self.db.replace(self.eid, data)

    def __getitem__(self, item):
        return self.db[self.eid][item]

    def delete(self):
        self.db.delete(self.eid)

    def __str__(self):
        return self.db[self.eid]

    def __repr__(self):
        return self.db[self.eid]


class DB:

    def __init__(self, db_name, table_name=None, port=27017):

        if not table_name:
            if '.' not in db_name:
                raise RuntimeError("use DB('dbName.tableName')")
            db_name, table_name = db_name.split('.')
            if {db_name, table_name} & {'', ' '}:
                raise RuntimeError("dbName or tableName should not be blank")
        self.db_name, self.table_name = db_name, table_name

        self.io = pymongo.MongoClient(f"mongodb://localhost:{port}")

        if self.table_name not in self.io[self.db_name].list_collection_names():
            print(f"table '{self.db_name}.{self.table_name}' not found, created")

        self.db = self.io[self.db_name][self.table_name]

    def __getitem__(self, eid):
        return self.get(eid)

    def __contains__(self, item):
        if type(item) != dict:
            return len(list(self.db.find({'_id': item})))
        else:
            return len(list(self.db.find({'_id': item["id"]})))

    def __call__(self, eid):
        return DBE(self, eid)

    def close(self):
        self.io.close()

    def __str__(self):
        return f"DB '{self.db_name}.{self.table_name}'"

    def __repr__(self):
        return f"DB '{self.db_name}.{self.table_name}'"

    def get(self, eid):
        return self.db.find_one(eid)

    def delete(self, eid):
        return self.db.delete_one(eid)

    def insert(self, eid, data=None):

        if type(eid) is dict:
            data = eid
            return self.db.insert_one(data)

        if data is None:
            data = {}
        
        data.update({'_id': eid})
        return self.db.insert_one(data)

    def update(self, eid, data=None):

        if type(eid) is dict:
            data = eid
            if '_id' not in data:
                raise RuntimeError('dict to update should have key `_id`')
            return self.db.update_one({'_id': data['_id']}, {'$set': data})

        if data is None:
            data = {}
        if '_id' in data and data['_id'] != eid:
            raise RuntimeError('_id can not be changed')
        return self.db.update_one({'_id': eid}, {'$set': data})

    def replace(self, eid, data=None):
        if type(eid) is dict:
            data = eid
            if '_id' not in data:
                raise RuntimeError('dict to replace should have key `_id`')
            return self.db.replace_one({'_id': data['_id']}, {'$set': data})

        if data is None:
            data = {}
        if '_id' in data and data['_id'] != eid:
            raise RuntimeError('_id can not be changed')
        return self.db.replace_one({'_id': eid}, {'$set': data})

    def find(self, *args, **kwargs):
        """
            k == v - {k: v}
            k != v - {k: {'$ne': v}}
            k > v - {k: {'$gt': v}}
            k < v - {k: {'$lt': v}}
            k >= v - {k: {'$gte': v}}
            k <= v - {k: {'$lte': v}}
            k in v - {k: {'$in': v}}
            k not in v - {k: {'$nin': v}}
            k re: v - {k: {'$regex': v}}
            A or B - {'$or': [A, B]}
            A and B - {'$and': [A, B]}
            k contains v - {k: {'$elemMatch': {$eq: v}}}
        """
        if len(args) == 0:
            return self.db.find({})
        if type(args[0]) is str:
            q = args[0]
            if ' == ' in q:
                index = q.index(' == ')
                k, v = q[:index], eval(q[index + 4:])
                return self.find({k: v}, **kwargs)
            elif ' != ' in q:
                index = q.index(' != ')
                k, v = q[:index], eval(q[index + 4:])
                return self.find({k: {'$ne': v}}, **kwargs)
            elif ' > ' in q:
                index = q.index(' > ')
                k, v = q[:index], eval(q[index + 3:])
                return self.find({k: {'$gt': v}}, **kwargs)
            elif ' < ' in q:
                index = q.index(' < ')
                k, v = q[:index], eval(q[index + 3:])
                return self.find({k: {'$lt': v}}, **kwargs)
            elif ' >= ' in q:
                index = q.index(' >= ')
                k, v = q[:index], eval(q[index + 4:])
                return self.find({k: {'$gte': v}}, **kwargs)
            elif ' <= ' in q:
                index = q.index(' <= ')
                k, v = q[:index], eval(q[index + 4:])
                return self.find({k: {'$lte': v}}, **kwargs)
            elif ' in ' in q:
                index = q.index(' in ')
                k, v = q[:index], eval(q[index + 4:])
                return self.find({k: {'$in': v}}, **kwargs)
            elif ' not in ' in q:
                index = q.index(' not in ')
                k, v = q[:index], eval(q[index + 8:])
                return self.find({k: {'$nin': v}}, **kwargs)
            elif ' re: ' in q:
                index = q.index(' re: ')
                k, v = q[:index], q[index + 5:]
                return self.find({k: {'$regex': v}}, **kwargs)
            elif ' contains ' in q:
                index = q.index(' contains ')
                k, v = q[:index], eval(q[index + 10:])
                return self.find({k: {'$elemMatch': {'$eq': v}}}, **kwargs)

        return self.db.find(*args, **kwargs)

    def sort(self, key, reverse=False):
        return self.db.find().sort(key, -1 if reverse else 1)


if __name__ == '__main__':
    tdb = DB('')
    for each in tdb.find('', limit=10):
        print(each)
    tdb.close()

