# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
from configparser import ConfigParser


def make_dir(path):
    """
    Makes directory based on path.
    :param path:
    :return:
    """
    try:
        os.makedirs(path)
    except FileExistsError:
        pass


def get_pars_from_ini(file_name='variables.ini'):
    """
    Returns dictionary with data for creating an xarray dataset from hdf5 file
    :param file_name: campaign from data comes from
    :type file_name: str
    :return: data from config files
    """
    path = f"{os.path.abspath(os.path.join(os.path.abspath(''), '../'))}"
    file_name = f"{path}/config/{file_name}"
    parser = ConfigParser()
    parser.optionxform = str
    parser.read(file_name)

    dt_pars = {}

    groups = parser.sections()

    for group in groups:
        db = {}
        params = parser.items(group)

        for param in params:
            try:
                db[param[0]] = eval(param[1])

            except ValueError:
                db[param[0]] = param[1].strip()

            except NameError:
                db[param[0]] = param[1].strip()

            except SyntaxError:
                db[param[0]] = param[1].strip()

        dt_pars[group] = db

    return dt_pars


def var_to_dict(standard_name, data, units, long_name):
    """
    Convert variable information to a dictionary.
    """
    d = {"data": data[:], "units": units, "long_name": long_name, "standard_name": standard_name}
    return d
