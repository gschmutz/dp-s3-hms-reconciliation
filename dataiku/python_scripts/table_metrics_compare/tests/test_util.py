import pytest
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from util import replace_vars_in_string

def test_replace_single_variable():
    s = "file_{database}.csv"
    variables = {"database": "testdb"}
    result = replace_vars_in_string(s, variables)
    assert result == "file_testdb.csv"

def test_replace_multiple_variables():
    s = "s3://{bucket}/{database}/{table}.csv"
    variables = {"bucket": "mybucket", "database": "db1", "table": "tbl1"}
    result = replace_vars_in_string(s, variables)
    assert result == "s3://mybucket/db1/tbl1.csv"

def test_missing_variable_keeps_placeholder():
    s = "file_{database}_{table}.csv"
    variables = {"database": "testdb"}
    result = replace_vars_in_string(s, variables)
    assert result == "file_testdb_{table}.csv"

def test_no_variables_in_string():
    s = "file.csv"
    variables = {"database": "testdb"}
    result = replace_vars_in_string(s, variables)
    assert result == "file.csv"

def test_empty_variables_dict():
    s = "file_{database}.csv"
    variables = {}
    result = replace_vars_in_string(s, variables)
    assert result == "file_{database}.csv"