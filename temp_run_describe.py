import os

os.environ["DQ_DESCRIBE_DUMP"] = "/tmp/dq.txt"
import scripts.describe_dq_tables as helper

helper.main()
