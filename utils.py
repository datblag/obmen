import logging


def check_docid(prm_row, prm_is_filial):
    if prm_is_filial == 0:
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

    return res
