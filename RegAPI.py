"""
A wrapper around the regulations.gov API.

**General Workflow**

Once you have an `api` instance, you can build a request using one "url constructor" (.endpoint, .url) and 0 or more "search modifiers" (.search, .page, .sort, .lastmodified). Then, to get the response call `.get()` on the object.
"""
from datetime import datetime
import requests
import time
from copy import deepcopy


class RegAPI:
    def __init__(
        self, 
        page_size=250, 
        apikeys=["e7LVpmbLfa0f0dDAx6TPzg86cG5TASGTafkxQHWg"]
    ):
        self.apibase = "https://api.regulations.gov/v4"

        self.apikeys = apikeys
        self.apikey_idx = 0
        self.tried_keys = 0

        self.page_size = page_size

        self.reqstr = ""
        self.ratelimit = 1000

    def _key(self):
        return self.apikeys[self.apikey_idx]

    def _next_key(self):
        self.apikey_idx = (self.apikey_idx + 1) % len(self.apikeys)

    def _add_apikey(self):
        self.reqstr = self.reqstr + f"?api_key={self._key()}"
        return self

    ### Main url constructors
    def endpoint(self, endpoint):
        self.reqstr = f"{self.apibase}{endpoint}"
        self._add_apikey()
        return self

    def url(self, url):
        self.reqstr = url
        self._add_apikey()
        return self

    ### Search modifiers
    def search(self, search_term):
        self.reqstr = self.reqstr + f"&filter[searchTerm]={search_term}"
        return self

    def page(self, page):
        self.reqstr = self.reqstr + f"&page[size]={self.page_size}&page[number]={page}"
        return self

    def sort(self, field):
        self.reqstr = self.reqstr + f"&sort={field}"
        return self

    def lastmodified(self, date, mod="ge"):
        if isinstance(date, str):
            date = datetime.fromisoformat(date)
        if isinstance(date, datetime):
            date = date.strftime("%Y-%m-%d %H:%M:%S")
        else:
            raise ValueError(f"date {date} is not a string or datetime object")
        self.reqstr = self.reqstr + f"&filter[lastModifiedDate][{mod}]={date}"
        return self
        

    ### Get response(s)
    def get(self, get_json=True):
        response = requests.get(self.reqstr)

        if response.status_code == 429:
            if self.tried_keys >= len(self.apikeys):
                self.tried_keys = 0
                raise RuntimeError("Rate-Limit exceeded")
            else:
                self._next_key()
                self.tried_keys += 1
                return self.get(get_json)

        if "X-Ratelimit-Remaining" in response.headers:
            self.ratelimit = int(response.headers["X-Ratelimit-Remaining"])

        if response.status_code >= 400:
            raise ConnectionError(f"GET failed with code <{response.status_code}> for request: {self.reqstr}")

        if get_json:
            response = response.json()
        self.reqstr = ""
        return response

    ### Clone
    def clone(self):
        return deepcopy(self)

def insert(doc, collection):
    doc["_inserted_time"] = time.time()
    doc["_idx"] = collection.count_documents({})
    collection.insert_one(doc)

def exists(document, collection):
    return collection.count_documents({"id": document["id"]}, limit=1) != 0

def getAll(apibasereq, collection, checkpoint_collection):
    pageNum = 1
    metaPageNum = 1
    date = None

    apidatereq = apibasereq.clone()
    if checkpoint_collection.count_documents({}) > 0:
        date = checkpoint_collection.find().next()["lastmodifiedDate"]
        apidatereq.lastmodified(date)

    while True: 
        apireq = apidatereq.clone()
        try:
            documents = apireq.sort("lastModifiedDate").page(pageNum).get()
            print(f"[{metaPageNum}](pg {pageNum}/40) ratelimit={apireq.ratelimit}", end=(" "*100)+"\r")
        except RuntimeError:
            print("Rate Limit exceeded, retrying in 1 minute")
            time.sleep(60)
            continue
        except ConnectionError as e:
            print(e)
            continue

        if len(documents["data"]) == 0:
            break

        for doc in documents["data"]:
            if exists(doc, collection):
                continue
            insert(doc, collection)

        if documents["meta"]["hasNextPage"] == False:
            if date is not None:
                checkpoint_collection.delete_one({})
                checkpoint_collection.insert_one({"lastmodifiedDate": date})
            date = documents["data"][-1]["attributes"]["lastModifiedDate"]
            apidatereq = apibasereq.clone().lastmodified(date)
            pageNum = 1
            metaPageNum += 1
        else:
            pageNum += 1
