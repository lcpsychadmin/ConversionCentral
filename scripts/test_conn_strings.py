from databricks.connect import DatabricksSession

samples = [
    "sc://foo.com/;token=abc",
    "sc://foo.com;token=abc",
    "sc://foo.com/;token=abc;x-databricks-http-path=sql/1.0/warehouses/xyz",
    "sc://foo.com/;token=abc;x-databricks-http-path=/sql/1.0/warehouses/xyz",
    "sc://foo.com:443/;token=abc;x-databricks-http-path=/sql/1.0/warehouses/xyz",
    "sc://foo.com:443;token=abc;x-databricks-http-path=/sql/1.0/warehouses/xyz",
    "sc://foo.com:443/;token=abc;x-databricks-http-path=sql%2F1.0%2Fwarehouses%2Fxyz",
]

for conn in samples:
    try:
        DatabricksSession.builder.remote(conn)
        print(conn, "-> accepted")
    except Exception as exc:
        print(conn, "->", type(exc).__name__, exc)
