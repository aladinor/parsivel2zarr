#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import xarray as xr
import io
import glob
import os
import pydsd as pyd
import matplotlib.pyplot as plt
from utils import get_pars_from_ini, make_dir
from re import split


def main():
    location = split(', |_|-|!', os.popen('hostname').read())[0].replace("\n", "")
    path_data = get_pars_from_ini(file_name='loc.ini')[location]['path_data']
    data = f'{path_data}/parsivel/data/0035215020'
    txt_files = glob.glob(f'{data}/****/***/**/*.txt')
    dsd = pyd.read_parsivel(txt_files[1364])
# #    path_save = 'C:/Users/alfonso8/Documents/python/parsivel/zarr'
#     path_save = '/data/keeling/a/alfonso8/gpm/parsivel/zarr'
#     make_dir(path_save)
#     _ = ds.to_zarr(store=f'{path_save}', consolidated=True, mode='w')
#     print('done!')

    ls_ds = [Parsivel(i, save="../res").txt2xr() for i in txt_files[:2000] if os.path.getsize(i) > 0]
    ds = xr.merge(ls_ds)
    dsd.calculate_dsd_parameterization()
    fig = plt.figure(figsize=(8, 8))

    # pyd.plot.plot_dsd(dsd)
    pyd.plot.plot_NwD0(dsd)
    plt.title('Drop Size Distribution')
    plt.plot(dsd.time, dsd.fields['Zh']['data'])
    pass


if __name__ == "__main__":
    main()
