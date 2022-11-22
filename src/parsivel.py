#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import numpy as np
import xarray as xr
import io
import glob
import os
import zarr
from pandas import to_datetime, to_numeric
from utils import get_pars_from_ini, make_dir
from re import split


def ds2zarr(ds, store='../zarr', **kwargs):
    """
    Functions that storage a xarray dataset into a zarr storage file
    :param store: zarr store path
    :param ds: xarray dataset to be stored
    :return :
    """
    store = zarr.DirectoryStore(store)
    args = {'consolidated': True}
    try:
        ds.to_zarr(store=store, **args)
    except zarr.errors.ContainsGroupError:
        args['mode'] = 'a'
        if not hasattr(ds, 'time'):
            args['append_dim'] = 'params'
        else:
            args['append_dim'] = 'time'
        ds.to_zarr(store=store, **args)


class Parsivel(object):
    def __init__(self, ls_path):
        self.path = ls_path
        self.data, self.type = self._get_data()
        self.vars = self._read_config()
        self.time = None
        self._base_time = None

    @staticmethod
    def _read_config():
        return get_pars_from_ini()

    def _get_data(self):
        with io.open(self.path, 'r', encoding="latin-1") as f:
            lines = f.readlines()
            try:
                _vars = {i.rstrip('\n\r;').split(':')[0]: (i.rstrip('\n\r;').split(':')[0]
                                                           if len(i.rstrip('\n\r;').split(':')) == 1
                                                           else ''.join(i.rstrip('\n\r;').split(':')[1:])) for i in
                         lines}
                return _vars, lines[0].rstrip('\n\r;')
            except IndexError:
                raise Exception(f"Empty file {self.path}. Please use a non-empty file")

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
                    if i == '20':
                        self.time = to_datetime(data[i], format='%H%M%S')
                    elif i == '21':
                        self._base_time = to_datetime(data[i], format='%d.%m.%Y')
                    else:
                        _val = to_datetime(data[i], format='%H%M%S %d.%m.%Y').to_datetime64()
                else:
                    try:
                        _val = to_numeric(data[i])
                        xr_data[table[i]['short_name']] = (['time'], np.array([np.where(_val == -9.999,
                                                                                        np.nan, _val)]))
                    except ValueError:
                        _val = np.fromstring(data[i], sep=';')
                        if table[i]['short_name'] == 'vd':
                            xr_data[table[i]['short_name']] = (['time', 'velocity'], np.array([_val]))
                        elif table[i]['short_name'] == 'raw':
                            try:
                                _val = _val.reshape(32, 32)
                                xr_data[table[i]['short_name']] = (['time', 'diameter', 'velocity'], np.array([_val]))
                            except ValueError:
                                print(f"Corrupted file. {self.path}")
                                pass
                        else:
                            if i == '90':
                                if _val.shape[0] != 32:
                                    print(f"Corrupted file. {self.path}")
                                    pass
                                else:
                                    xr_data[table[i]['short_name']] = (['time', 'diameter'],
                                                                       np.array([10 ** np.where(_val == -9.999,
                                                                                                np.nan, _val)]))
                            else:
                                xr_data[table[i]['short_name']] = (['time', 'diameter'],
                                                                   np.array([np.where(_val == -9.999, np.nan, _val)]))
            else:
                # TO DO: Make sure what are the fields not recognized in previous block
                attrs[i] = f'{data[i]}'
        attrs['d_sizes'] = np.array(self.vars['diameter']['d_diam'])
        attrs['v_sizes'] = np.array(self.vars['velocity']['d_vel'])

        vel = np.array(self.vars['velocity']['vel'])
        diameter = np.array(self.vars['diameter']['diam'])
        coords = dict(time=('time', np.array(
            [to_datetime(f"{self.time.strftime('%X')} {self._base_time.strftime('%Y%m%d')}").to_datetime64()
             ])),
                      diameter=(['diameter'], diameter),
                      velocity=(['velocity'], vel))
        ds = xr.Dataset(
            data_vars=xr_data,
            coords=coords,
            attrs=attrs
        )
        return ds


def main():
    location = split(', |_|-|!', os.popen('hostname').read())[0].replace("\n", "")
    path_data = get_pars_from_ini(file_name='loc.ini')[location]['path_data']
    data = f'{path_data}/parsivel/data/0035215020'
    path_save = f"{path_data}/parsivel/zarr/{data.split('/')[-1]}"
    folders = list(filter(os.path.isdir, glob.glob(f'{data}/**/*', recursive=True)))
    for idx, j in enumerate(folders):
        ls_ds = [Parsivel(i).txt2xr() for i in glob.glob(f'{j}/*.txt') if os.path.getsize(i) > 0]
        if ls_ds:
            ds = xr.merge(ls_ds)
            ds2zarr(ds, store=path_save)
    print('done!')
    pass


if __name__ == "__main__":
    main()
