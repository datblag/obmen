import logging
from config import filial_sklad_white_list, filial_firma_white_list


def is_process_doc(prm_closed):
    return prm_closed & 1
    # 0-непроведен
    # 1-проведен
    # 4-непроведен
    # 5-проведен


def convert_base(num, to_base=10, from_base=10):
    # first convert to decimal number
    if isinstance(num, str):
        n = int(num, from_base)
    else:
        n = int(num)
    # now convert decimal to 'to_base' base
    alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    if n < to_base:
        return alphabet[n]
    else:
        return convert_base(n // to_base, to_base) + alphabet[n % to_base]


def check_docid(prm_row, prm_is_filial):
    #if prm_is_filial == 0:
    if not prm_row['idartmarket'] == None and not prm_row['idartmarket'].strip() == '':
        return True
    else:
        logging.error(';'.join(['Пустой ид', prm_row['docno']]))
        return False


def check_firma(prm_row, prm_is_filial):
    if prm_is_filial == 0:
        if prm_row['firma'] == '9CD36F19-B8BD-49BC-BED4-A3335D2175C2    ':
            return True
        else:
            return False
    if prm_is_filial == 1:
        if not prm_row['firma'] is None and prm_row['firma'].strip() in filial_firma_white_list:
            return True
        else:
            return False

def check_client(prm_row, prm_is_filial, prm_isclosed):
    if not prm_row['client'] is None and not prm_row['client'] == '':
        return True
    else:
        if prm_isclosed == 1:
            logging.error(';'.join(['Пустой клиент', prm_row['docno']]))
        return False


def check_sklad(prm_row, prm_is_filial):
    res = None
    if prm_is_filial == 0:
        if not prm_row['sklad'] is None and not prm_row['sklad'].strip() == '':
            res = True
        else:
            res = False
            logging.error(';'.join(['Пустой склад', prm_row['docno']]))

        if 'sklad_in' in prm_row and res:
            if not prm_row['sklad_in'] is None and not prm_row['sklad_in'].strip() == '':
                res = True
            else:
                logging.error(';'.join(['Пустой склад получатель', prm_row['docno']]))
                res = False

    if prm_is_filial == 1:
        if not prm_row['sklad'] is None and not prm_row['sklad'].strip() == '' and prm_row['sklad'].strip() in filial_sklad_white_list:
            res = True
        else:
            res = False
            logging.error(';'.join(['Пустой склад', prm_row['docno']]))

        if 'sklad_in' in prm_row and res:
            if not prm_row['sklad_in'] is None and not prm_row['sklad_in'].strip() == '' and prm_row['sklad_in'].strip() in filial_sklad_white_list:
                res = True
            else:
                logging.error(';'.join(['Пустой склад получатель', prm_row['docno']]))
                res = False

    return res
