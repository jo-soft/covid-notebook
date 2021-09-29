import pandas as pd
url="https://www.arcgis.com/sharing/rest/content/items/f10774f1c63e40168479a1feb6c7ca74/data"

def get_data():
    ds = pd.read_csv(url)
    ds.Meldedatum = pd.to_datetime(ds.Meldedatum, format="%Y/%m/%d %H:%M:%S")
#    ds.reset_index()
#    ds.set_index(['Meldedatum', 'IdLandkreis'], inplace=True)
    return ds
