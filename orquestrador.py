# -*- coding: utf-8 -*-
import os
import load_stg
import load_tables


def main():
    os.system('python3.6 {}'.format(load_stg.__file__))
    os.system('python3.6 {}'.format(load_tables.__file__))


if __name__ == '__main__':
    main()