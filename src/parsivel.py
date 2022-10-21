import pandas as pd
import xarray as xr
import glob




def get_vars(f):
    _ = '\n'
    _vars = {f"{i.split(':')[0].replace(f'{_}', '')}": f"{i.split(':')[-1].replace(f'{_}', '')}"
             for i in f.readlines()}
    return _vars


def main():
    variables = {'01': 'r_intensity', '02': 'acc_r', '03': 'Synop_4680', '04': 'Synop_4677', '05': ''}
    path = 'C:/Users/alfonso8/Downloads/0035215020/2021/10/02'
    txt_files = glob.glob(f'{path}/*.txt')
    with open(txt_files[0], 'r') as f:

        _vars['date'] = pd.Timestamp()
        lines = get_lines(f, [0, 1, 3, 8, 10, 13])
        lines = list(lines)
        gist_id = lines[0].rstrip()
        token = lines[1].rstrip()

        print(1)

    df = pd.read_csv(txt_files[0], delimiter='\n')
    print(1)
    pass


if __name__ == "__main__":
    main()