import flask
import json
import core

app = flask.Flask(__name__)


@app.route("/monitor/<username>/<filter>/<from_date>")
def show_tables(username, filter, from_date):
    data = core.data.get_data(username, filter, from_date)
    return flask.jsonify(data)


if __name__ == "__main__":
    import os

    if os.name == 'nt':
        host = os.environ["COMPUTERNAME"]
    else:
        host = 'localhost'

    app.run(host, 1337, debug=True)
