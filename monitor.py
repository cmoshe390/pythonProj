from flask import Flask, make_response, jsonify
from gevent.pywsgi import WSGIServer
import logging

app = Flask(__name__)


logger = logging.getLogger(__name__)


@app.route('/')
def health_check():
    try:
        return make_response(jsonify(success=True), 200)

    except Exception as e:
        print(f'error- {e}')
        return make_response(jsonify(success=False), 400)


if __name__ == '__main__':
    try:
        ip = "127.0.0.1"
        port = 8081
        http_server = WSGIServer((ip, port), application=app, log=app.logger, error_log=app.logger)
        print(f'Running on http://{ip}:{port}/')
        logger.info(f'Running on http://{ip}:{port}/')
        http_server.serve_forever()
    except Exception as e:
        print(f'error: {e}')
