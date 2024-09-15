#!/bin/sh
set -e

# run dbt
dbt run --profiles-dir . --target dev --vars "{client: $CLIENT_NAME, project: $GCP_PROJECT}"

# create the Flask app on the fly (optional: remove if unnecessary)
cat <<EOF > app.py
from flask import Flask
app = Flask(__name__)

@app.route('/')
def hello():
    return "dbt run completed successfully", 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=$PORT)
EOF

# start the Flask app using Gunicorn (production server)
exec gunicorn --bind 0.0.0.0:$PORT --workers 1 --threads 8 --timeout 0 app:app 

exit 0