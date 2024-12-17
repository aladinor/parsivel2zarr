#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import numpy as np
import xarray as xr
import io
import glob
import os
import zarr
from pandas import to_datetime, to_numeric
from utils import get_pars_from_ini
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
        time_enc = dict(
            units="nanoseconds since 1950-01-01T00:00:00.00",
            dtype="int64",
            _FillValue=-9999,
        )
        args["encoding"] = dict(time=time_enc)
        ds.to_zarr(store=store,
                   **args)
    except zarr.errors.ContainsGroupError:
        args.pop("encoding")
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
                        try:
                            self.time = to_datetime(data[i], format='%H%M%S')
                        except ValueError:
                            print(f"Non-compatible time format. {self.path}. Please make it compatible")
                            break
                    elif i == '21':
                        try:
                            self._base_time = to_datetime(data[i], format='%d.%m.%Y')
                        except ValueError:
                            print(f"Non-compatible base time format. {self.path}. Please make it compatible")
                            continue
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
                                _val = np.tile(np.nan, (32, 32))
                                xr_data[table[i]['short_name']] = (['time', 'diameter', 'velocity'], np.array([_val]))
                                print(f"Corrupted file. Raw data is not complete. {self.path}")
                        else:
                            if i == '90':
                                if _val.shape[0] != 32:
                                    _val = np.tile(np.nan, 32)
                                    print(f"Corrupted file. Nd data is not complete. {self.path}")
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
            try:
                attrs[table[i]['short_name']] = self.vars['variables'][i]
            except KeyError:
                pass
        try:
            attrs['d_sizes'] = np.array(self.vars['diameter']['d_diam'])
            attrs['v_sizes'] = np.array(self.vars['velocity']['d_vel'])

            vel = np.array(self.vars['velocity']['vel'])
            diameter = np.array(self.vars['diameter']['diam'])
            coords = dict(time=('time',
                                np.array(
                                    [to_datetime(
                                        f"{self.time.strftime('%X')} {self._base_time.strftime('%Y%m%d')}").to_datetime64()
                                     ])),
                          diameter=(['diameter'], diameter),
                          velocity=(['velocity'], vel))
            ds = xr.Dataset(
                data_vars=xr_data,
                coords=coords,
                attrs=attrs | self.vars
            )
            return ds
        except AttributeError:
            return None


def main():
    location = split(', |_|-|!', os.popen('hostname').read())[0].replace("\n", "")
    path_data = get_pars_from_ini(file_name='loc.ini')[location]['path_data']
    datas = list(filter(os.path.isdir, glob.glob(f'{path_data}/parsivel/data/*', recursive=True)))
    for data in datas:
        path_save = f"{path_data}/parsivel/zarr/{data.split('/')[-1]}"
        folders = list(filter(os.path.isdir, glob.glob(f'{data}/**/*', recursive=True)))
        for idx, j in enumerate(folders):
            ls_ds = [Parsivel(i).txt2xr() for i in glob.glob(f'{j}/*.txt') if os.path.getsize(i) > 0]
            if ls_ds:
                ls_ds = [i for i in ls_ds if i is not None]
                ds = xr.merge(ls_ds)
                ds2zarr(
                    ds,
                    store=path_save
                )
                
        print('done!')
    pass


if __name__ == "__main__":
    main()
