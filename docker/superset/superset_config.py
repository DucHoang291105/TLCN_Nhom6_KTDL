import os


SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = os.environ["SUPERSET_DATABASE_URI"]
WTF_CSRF_ENABLED = True
TALISMAN_ENABLED = False
ROW_LIMIT = 50_000
SUPERSET_WEBSERVER_PORT = 8088
FEATURE_FLAGS = {"ENABLE_TEMPLATE_PROCESSING": True}
# Vietnamese number style on charts: 13.021 and 1,3 (same as the dashboards' finding cards).
D3_FORMAT = {"decimal": ",", "thousands": ".", "grouping": [3], "currency": ["", " đ"]}
