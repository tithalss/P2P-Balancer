from flask import Flask

app = Flask(__name__)

@app.route("/")
def home():
    return "Master rodando 🚀"

if __name__ == "__main__":
    app.run(port=5000)
