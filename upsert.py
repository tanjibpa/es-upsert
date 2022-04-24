import json

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError

es = Elasticsearch(hosts=["http://localhost:9200"])

def upsert(body):
    updated_body = {
        "script": {
            "source": "ctx._source.orders.add(params.order)",
            "lang": "painless",
            "params": {"order": body["order"]}
        },
        "upsert": {
            "username": body["username"],
            "orders": [body["order"]]
        },
        "scripted_upsert": True
    }
    res = es.update(index="orders", id=body["username"], body=updated_body)
    return res


if __name__ == "__main__":
    data = {
        "username": "user1",
        "order": {
            "id": "order1",
            "timestamp": "2022-01-01T00:00:00Z",
            "items": [
                {"id": "item1", "name": "item1", "price": 10, "quantity": 1},
                {"id": "item2", "name": "item2", "price": 20, "quantity": 2},
            ],
        },
    }
    print(upsert(data))