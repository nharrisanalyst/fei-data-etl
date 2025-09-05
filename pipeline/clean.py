def clean(df):
    print('cleaning')
    df.columns = df.columns.str.strip()
    df = df.applymap(lambda x: x.strip() if isinstance(x,str) else x)
    df = df.applymap(lambda x: x.replace(",","") if isinstance(x,str) else x)
    df = df.applymap(lambda x: 0 if x == "0" else x)
    df = df.applymap(lambda x: -int(x.strip('()')) if isinstance(x,str)  and x.endswith(')') and x.startswith('(')  else x)
    df = df.applymap(lambda x: int(x) if isinstance(x,str) and  "." not in x else x)
    df = df.applymap(lambda x: float(x) if isinstance(x,str) and  "."  in x else x)
    return df
