import numpy as np
import pandas as pd
if __name__ == '__main__':
    dst_stocks={'002350.XSHE': -10.090666666666669, '002365.XSHE': -8.1019999999999985, '600992.XSHG': -7.9606666666666666, '002576.XSHE': -15.513980000000004, '600051.XSHG': -4.1446666666666676, '600727.XSHG': -3.1819999999999995, '002319.XSHE': -9.5513333333333357, '000004.XSHE': -20.136666666666663, '600731.XSHG': -1.8213333333333335, '002034.XSHE': -7.9046666666666709, '600448.XSHG': -3.741313333333335}  
    print(dst_stocks)
    df = pd.DataFrame(data=list(dst_stocks.values()), index=dst_stocks.keys())
    df.columns = ['score']
    df = df.sort(columns='score', ascending=True)
    print(df)
    print(df.index)