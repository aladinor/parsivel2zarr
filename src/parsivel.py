#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import xarray as xr
import glob
import os
from utils import get_pars_from_ini, make_dir


class Parsivel(object):
    def __init__(self, ls_path, save):
        self.path = ls_path
        self.save = save
        self.date = pd.to_datetime(self.path.split('_')[-1].split('.')[0])
        self.data, self.type = self._get_data()
        self.vars = self._read_config()

    @staticmethod
    def _read_config():
        return get_pars_from_ini()

    def _get_data(self):
        with open(self.path, 'r') as f:
            lines = f.readlines()
            if len(lines) != 0:
                _: str = '\n'
                _vars = {f"{i.split(':')[0].replace(f'{_}', '')}": f"{i[3:].replace(f'{_}', '')}"
                         for i in lines[1:] if len(i) > 3}
                return _vars, lines[0].replace(f'{_}', '')
            else:
                print(self.path)
                return None, None

    def txt2xr(self):
        data = self.data
        table = self.vars['variables']

        xr_data = {}
        attrs = {}
        for i in list(data.keys()):
            try:
                if table[i]['short_name'].startswith('unknown'):
                    attrs[i] = f'{data[i]}'
                    continue
            except KeyError:
                continue

            if i in table.keys():
                _dt = table[i]['dtype']
                if _dt == 'str':
                    _val = data[i].replace(' ', '')
                elif _dt == 'timestamp':
                    _val = pd.Timestamp(data[i]).to_datetime64()
                else:
                    try:
                        _val = pd.to_numeric(data[i])
                        xr_data[table[i]['short_name']] = (['time'], np.array([_val])) # eerror aca
                    except ValueError:
                        _val = np.fromstring(data[i], sep=';')
                        if table[i]['short_name'] == 'vd':
                            xr_data[table[i]['short_name']] = (['time', 'velocity'], np.array([_val]))
                        elif table[i]['short_name'] == 'raw':
                            _val = _val.reshape(32, 32)
                            xr_data[table[i]['short_name']] = (['time', 'diameter', 'velocity'], np.array([_val]))
                        else:
                            xr_data[table[i]['short_name']] = (['time', 'diameter'], np.array([_val]))
            else:
                attrs[i] = f'{data[i]}'

        vel = np.array(self.vars['velocity']['vel'])
        diameter = np.fromstring(self.vars['diameter']['diam'], sep='\t')
        coords = dict(time=('time', np.array([self.date.to_datetime64()])),
                      diameter=(['diameter'], diameter),
                      velocity=(['velocity'], vel))
        ds = xr.Dataset(
            data_vars=xr_data,
            coords=coords,
            attrs=attrs
        )
        return ds


def main():
    path = 'C:/Users/alfonso8/Downloads/0035215020'
    txt_files = glob.glob(f'{path}/***/**/*/*.txt')
    ls_ds = [Parsivel(i, save="../res").txt2xr() for i in txt_files[:1000] if os.path.getsize(i) > 0]
    ds = xr.merge(ls_ds)
    path_save = '../res/disdro_test'
    make_dir(path_save)
    ds.to_zarr(path_save)
    print(1)
    pass


if __name__ == "__main__":
    main()