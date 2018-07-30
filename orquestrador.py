# -*- coding: utf-8 -*-
import os
import sys

def main():

    import load_stg
    import load_tables

    os.system('python3.6 {}'.format(load_stg.__file__))
    os.system('python3.6 {}'.format(load_tables.__file__))


if __name__ == '__main__':
    try:
        os.system('pip3.6 install -r requirements.txt -t .')
        main()
    except:
        sys.exit('Error na instalação do requirements.')