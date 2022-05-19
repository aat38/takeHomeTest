# BELOW IS A SAMPLE OF WORK (CREATING AN API) THAT I'VE RECENTLY COMPLETED FOR GITHUB.
# THIS WAS A 2 HOUR TAKE HOME TEST. THE TASK EXPLAINATION IS SHOWN IN THE COMMENTED BLOCK BELOW


# # Coding Exercise: Data Storage API

# Implement a small HTTP service to store objects organized by repository.
# Clients of this service must implement the API below.


# We value your time, and ask that you spend no more than **2 hours** on this exercise.

# ## General Requirements

# * The service should identify objects by their content. This means that two objects with the same content should be considered identical, and only one such object should be stored per repository.
#   =>solving this by hashing each object's content and saving hash as oid
# * Two objects with the same content that are in separate repositories should be stored separately.
# * The included tests should pass and should not be modified.
# * Do not move or rename any of the existing files.
# * The service must implement the API as described below.
# * The data can be persisted in memory, on disk, or wherever you like.
#   => I am using database.json. it needs to be initialized with an empty pair of brackets
# * Prefer using the standard library over external dependencies. If you must include an external dependency, please explain your choice in the pull request.

# ## Recommendations

# * Your code will be read by humans, please take the time to optimize for that.
# * Add extra tests to test the functionality of your implementation.
# * The description of your submission should be used to describe your reasoning, your assumptions and the tradeoffs in your implementation.
# * If your chosen language allows for concurrency, remember that this is a web application and concurrent requests will come in.
# * Focus on getting a working solution and avoid external dependencies for data storage.

# ## API

# ### Upload an Object

# ```
# PUT /data/{repository}
# ```

# #### Response

# ```
# Status: 201 Created
# {
#   "oid": "2845f5a412dbdfacf95193f296dd0f5b2a16920da5a7ffa4c5832f223b03de96",
#   "size": 1234
# }
# ```

# ### Download an Object

# ```
# GET /data/{repository}/{objectID}
# ```

# #### Response

# ```
# Status: 200 OK
# {object data}
# ```

# Objects that are not on the server will return a `404 Not Found`.

# ### Delete an Object

# ```
# DELETE /data/{repository}/{objectID}
# ```

# #### Response

# ```
# Status: 200 OK
# ```

# ## Getting started and Testing

# This exercise requires a python 3.9. Get started by installing dependencies:

# ```sh
# pip install -r requirements.txt
# ```

# Write your server implementation in `server.py`. Then run the tests:

# ```sh
# python -m unittest server_test.py
# ```

# ## Submitting Your Work
# When you are finished:
#   - Commit all of your code.
#   - Push your changes to GitHub.
#   - Open a [pull request](https://help.github.com/articles/creating-a-pull-request/).
#   - Visit https://interviews.githubapp.com/ and click `Done`.
#   - Thank you! 🎉


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
