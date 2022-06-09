from flask import Flask, request, abort, send_from_directory, jsonify, redirect, url_for, Response
import pathlib
import decimal
import logging

app = Flask(__name__)
base = pathlib.Path(__file__).parent / "content"


def recursive_mkdir(path: pathlib.Path):
    if not path.exists():
        recursive_mkdir(path.parent)
        path.mkdir()


@app.route("/", methods=["GET", "PUT", "DELETE", "HEAD", "MOVE"])
def handle_root():
    return _handle_request("")


@app.route("/<path:content>", methods=["GET", "PUT", "DELETE", "HEAD", "MOVE"])
def handle_request(content):
    return _handle_request(content)


def _fix_decimal(a, b):
    return a, decimal.Decimal(b)


def select_content_type(server_options, accept_header):
    client_options = [(x.strip(" "), decimal.Decimal(1)) if ";" not in x else _fix_decimal(*x.strip(" ").split(";q=", maxsplit=1)) for x in accept_header.split(",")]
    acceptable_options = []
    best_weight = None
    client_options.sort(key=lambda x: x[1], reverse=True)
    for mime_type, weight in client_options:
        if best_weight and weight < best_weight:
            continue
        for mt, w in server_options:
            if mime_type == mt or mime_type == "*/*" or ("*" in mime_type and mt.startswith(mime_type[:mime_type.find("*")])):
                if best_weight is None:
                    best_weight = weight
                acceptable_options.append((mt, w))
    if not acceptable_options:
        return None
    acceptable_options.sort(key=lambda x: x[1], reverse=True)
    return acceptable_options[0][0]


def _handle_request(content):
    full_path = base / content
    # Don't let them outside of the content directory
    full_path = full_path.absolute()
    if not str(full_path).startswith(str(base)):
        return abort(404)
    if content and full_path.exists() and full_path.is_dir() and not content.endswith("/"):
        return redirect(url_for("handle_request", content=content + "/"), code=301)
    if request.method == "HEAD":
        if not full_path.exists():
            return abort(404)
        return Response(status=200)
    if request.method == "GET":
        if not full_path.exists():
            return abort(404)
        if full_path.is_dir():
            content_type = select_content_type([
                ("application/json", 1),
                ("text/html", 0.9)
            ], request.headers.get("Accept", "*/*"))
            if content_type == "application/json":
                response = jsonify([
                    x.name if x.is_file() else "{}/".format(x.name) for x in full_path.iterdir()
                ])
                response.status_code = 200
                return response
            elif content_type == "text/html":
                return abort(501)
            else:
                # Only HTML and application/json is acceptable
                return abort(406)
        return send_from_directory(full_path.parent, full_path.name)
    elif request.method == "PUT":
        if full_path.exists() and full_path.is_dir():
            response = jsonify({"success": False, "error": "The path is already a directory"})
            response.status_code = 500
            return response
        recursive_mkdir(full_path.parent)
        with open(full_path, "wb") as h:
            h.write(request.get_data())
        response = jsonify({"success": True})
        response.status_code = 200
        return response
    elif request.method == "DELETE":
        if not full_path.exists():
            return abort(404)
        if full_path == base:
            response = jsonify({"success": False, "error": "Cannot remove the base directory"})
            response.status_code = 500
            return response
        if full_path.is_file():
            full_path.unlink()
            response = jsonify({"success": True})
            response.status_code = 200
            return response
        else:
            for x in full_path.iterdir():
                response = jsonify({"success": False, "error": "The directory is not empty"})
                response.status_code = 500
                return response
            full_path.rmdir()
            response = jsonify({"success": True})
            response.status_code = 200
            return response



