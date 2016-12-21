#@PydevCodeAnalysisIgnore
import pandas as pd

if __name__ == '__main__':
    list1=['002020','000687']
    list2=['000241','000687']
    res_list=list(set(list2)&set(list1))
    if len(res_list):
        for res in res_list:
            list1.remove(res)
            list2.remove(res)
    df = pd.DataFrame(list1, index=list2)
    print(df)
    print(list2)   
