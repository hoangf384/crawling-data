import ast
import pandas as pd

def changing_id(df):
    """
    change data id to pid
    """
    if 'data_id' in df.columns:
        df = df.rename(columns={'data_id': 'pid'})
    if 'pid' in df.columns:
        df = df.set_index('pid')
    return df

def return_list(x):
    """
   tranform string to list.
    """
    if isinstance(x, str) and x.startswith('[') and x.endswith(']'):
        try:
            x = ast.literal_eval(x)
        except:
            pass
    return x
