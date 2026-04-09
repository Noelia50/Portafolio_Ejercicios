# utils.py
import pandas as pd

def calcular_edat(data_naix):
    """
    Calcula l'edat a partir d'una data de naixement.
    
    intput:
    data_naix : pd.Timestamp
        La data de naixement de l'usuari.
    
    output:
    L'edat en anys, tenint en compte si l'usuari ja ha fet anys aquest any.
    """
    avui = pd.Timestamp('today').normalize()  # data d'avui sense hora
    edat = avui.year - data_naix.year  # diferència en anys

    # Si encara no ha fet anys aquest any, restem 1
    if (avui.month, avui.day) < (data_naix.month, data_naix.day):
        edat -= 1

    return edat