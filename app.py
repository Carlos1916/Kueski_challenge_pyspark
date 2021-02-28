from flask import Flask
from etl.common import GenericETL
from etl.kueski_challenge import KueskiChallengeETL
app = Flask(__name__)


@app.route('/')
def home():
    return "<h1>hello!</h1>"


@app.route('/api/test')
def test():
    etl = GenericETL()
    return etl.run()


@app.route('/api/kueski_etl')
def kueski_api():
    etl = KueskiChallengeETL()
    return etl.run()

if __name__ == '__main__':
    app.run()