from importlib import import_module
from importlib.util import find_spec
from typing import Any

DatabricksSession: Any | None
try:
    spec = find_spec("databricks.connect")
except ModuleNotFoundError:
    spec = None

if spec is None:
    DatabricksSession = None
else:
    DatabricksSession = import_module("databricks.connect").DatabricksSession

samples = [
    "sc://foo.com/;token=abc",
    "sc://foo.com;token=abc",
    "sc://foo.com/;token=abc;x-databricks-http-path=sql/1.0/warehouses/xyz",
    "sc://foo.com/;token=abc;x-databricks-http-path=/sql/1.0/warehouses/xyz",
    "sc://foo.com:443/;token=abc;x-databricks-http-path=/sql/1.0/warehouses/xyz",
    "sc://foo.com:443;token=abc;x-databricks-http-path=/sql/1.0/warehouses/xyz",
    "sc://foo.com:443/;token=abc;x-databricks-http-path=sql%2F1.0%2Fwarehouses%2Fxyz",
]


def main() -> None:
    if DatabricksSession is None:
        print("Databricks SDK not installed; skipping connection checks.")
        return

    for conn in samples:
        try:
            DatabricksSession.builder.remote(conn)
            print(conn, "-> accepted")
        except Exception as exc:  # noqa: BLE001 - intentional debug output
            print(conn, "->", type(exc).__name__, exc)


if __name__ == "__main__":
    main()
