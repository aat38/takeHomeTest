import os
import hashlib
from typing import Callable, Any, List
from wsgiref.simple_server import make_server
import json


class DataStorageServer:

    def __init__(self) -> None:
        pass

    def __call__(self, environ: List[str], start_response: Callable[..., Any]) -> bytes:
        """
        The environ parameter is a dictionary containing some useful
        HTTP request information such as: REQUEST_METHOD, CONTENT_LENGTH,
        PATH_INFO, CONTENT_TYPE, etc.
        For the full list of attributes refer to wsgi definitions:
        https://wsgi.readthedocs.io/en/latest/definitions.html
        """

        if environ["REQUEST_METHOD"] == "GET":
            response_headers = [("Content-Type", "text/plain")]
            try:
                data = getDB()
                splitPath = environ["PATH_INFO"].split("/")
                if len(splitPath) >= 3:
                    repo = splitPath[2]
                    oid = splitPath[3]
                    if repo in data.keys() and oid in data[repo].keys():
                        status = "200 OK"
                        body = (data[repo][oid]).encode('utf-8')
                    else:
                        status = "404 NOT FOUND"
                        body = b"objectID not present in database"
                else:
                    status = "400 BAD REQUEST"
                    body = b"need to send an objectID"
            except:
                status = "500 ERROR"
                body = b"unable to complete get request"

        if environ["REQUEST_METHOD"] == "PUT":
            response_headers = [("Content-Type", "text/plain")]
            length = int(environ.get('CONTENT_LENGTH', '0'))
            repo = environ["PATH_INFO"].split("/")[2]
            if length >= 1:
                body = environ['wsgi.input'].read(length)
                bodyData = str(body).split("'")[1]
                oid = abs(int.from_bytes(hashlib.sha256((repo.encode('utf-8'))+body).digest()[
                    :8], byteorder='big', signed=True))
                # I used this hashing method to ensure consistent hashing even if server restarted. python's standard hash() does not use a fixed seed
                # https://stackoverflow.com/questions/30585108/disable-hash-randomization-from-within-python-program
                try:
                    writeDB(repo, oid, bodyData)
                    status = "201 CREATED"
                    body = json.dumps(
                        {'oid': str(oid), 'size': len(bodyData)}).encode('utf-8')
                except:
                    status = "500 ERROR"
                    body = b"unable to complete request"
            else:
                status = "400 BAD REQUEST"
                body = b"no body was sent so no changes were made"

        if environ["REQUEST_METHOD"] == "DELETE":
            response_headers = [("Content-Type", "text/plain")]
            splitPath = environ["PATH_INFO"].split("/")
            if len(splitPath) > 3:
                repo = splitPath[2]
                oid = splitPath[3]
                try:
                    status, body = delDB(repo, oid)
                except:
                    status = "500 ERROR"
                    body = b"unable to complete request"
            else:
                status = "400 BAD REQUEST"
                body = b"need to send an objectID"

        start_response(status, response_headers)
        yield body


def getDB() -> json:
    with open('database.json', 'r') as f:
        return json.load(f)


def writeDB(repo: str, oid: str, bodyData: str):
    data = getDB()
    newData = {str(oid): str(bodyData)}
    if repo in data.keys():
        data[repo].update(newData)
    else:
        data.update({repo: newData})
    with open('database.json', 'w') as f:
        json.dump(data, f)


def delDB(repo: str, oid: str):
    data = getDB()
    if repo in data.keys() and oid in data[repo].keys():
        data[repo].pop(oid)
        with open('database.json', 'w') as f:
            json.dump(data, f)
        return ["200 OK", b"objectID deleted"]
    return ["404 NOT FOUND", b"objectID not found in database"]


if __name__ == "__main__":
    app = DataStorageServer()
    port = os.environ.get('PORT', 8282)
    with make_server("", port, app) as httpd:
        print(f"Listening on port {port}...")
        httpd.serve_forever()
