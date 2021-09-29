import numpy as np
import pandas as pd
import geopandas as gpd

from functools import reduce

import covid19.dataService

def get_lk_data(path='./landkreise_deutschland__with_wiki_data__simplified20.geojson'):
    lk_data = gpd.read_file(path)
    lk_data['NEIGHBORS'] = None
    for index, lk in lk_data.iterrows():   

        # get 'not disjoint' countries
        neighbors = lk_data[~lk_data.geometry.disjoint(lk.geometry)]

        # add names of neighbors as NEIGHBORS value
        lk_data.at[index, "NEIGHBORS"] = neighbors
    return lk_data

def get_covid_data():
    covid_ds = covid19.dataService.get_data()
    return covid_ds

class CovidDataSrc(object):
    
    def __init__(self):
        print('Loading lk data')
        self._lk_data = get_lk_data()
        print('done')
        print('loading covid data')
        self._covid_ds = get_covid_data()
        print('done')
        
    def _group_cases(self, covid_ds, bys=['Meldedatum']):
        reduced_ds = covid_ds[bys +  ['AnzahlFall']]
        grouped_ds = reduced_ds.groupby(bys).agg({'AnzahlFall': np.sum})
        
        return grouped_ds
    
    def get_covid_data_for_lk(self, lk):
        lk_id = int(self._lk_data.loc[self._lk_data.GEN==lk].AGS.iloc[0])
        res = self._group_cases(self._covid_ds.loc[self._covid_ds.IdLandkreis==lk_id])
        res['cum_sum'] = res['AnzahlFall'].cumsum()
        return res
    
    def get_covid_data_for_lk_with_neighbours(self, lk):
        neigbours = self._lk_data.loc[self._lk_data.GEN==lk]['NEIGHBORS'].iloc[0]
        neigbours_ags = neigbours.AGS.map(int)
        neigbours_gen = neigbours.GEN
        covid_ds_for_neigbours = [
            self._group_cases(self._covid_ds.loc[self._covid_ds.IdLandkreis==neighbour_ags])
            .rename(columns={'AnzahlFall': f'AnzahlFall_{neighbour_gen}'})
            for [neighbour_ags, neighbour_gen] in zip(neigbours_ags, neigbours_gen)
        ]
        
        
        return reduce(lambda ds1, ds2: pd.merge(ds1, ds2, on='Meldedatum'), covid_ds_for_neigbours)
    
    def get_covid_data_of_date(self, date, lks=None):
        """
        If no lk is given return data of all lks
        data will be compared as pandas timestamp, pass a string
        """
        lk_ids = []
        if lks:
            lk_ids = [
                int(self._lk_data.loc[self._lk_data.GEN==lk].AGS.iloc[0])
                for lk in lks
            ]
        res = self._group_cases(
            self._covid_ds.loc[self._covid_ds.Meldedatum==date],
            bys=['IdLandkreis']
        )
        return res