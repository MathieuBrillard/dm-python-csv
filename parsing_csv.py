import time # pour tester le temps d'exécution
import os # pour gérer les répertoires/chemins
import dask.dataframe as dd # pour gérer les fichier csv


def ask_path():
    print("Voulez vous spécifier un path ? (default = 'data/data.csv')")
    reponse = input("y/n ?")
    if reponse == "y":
        path = input("Specifiez le path: ")
        return path
    else:
        path = 'data/data.csv'
        return path

def open_file(path) :
    """ Fonction permettant d'ouvrir un document excel et retournant la feuille active.
    return: l'objet du fichier csv chargé par dask.dataframe
    path: (str) chemin d'accès relatif au fichier à charger, à partir du dossier où est lancé le script.
        si "path" n'est pas spécifié, le chemin par défaut est "data/data.csv". 
    """
    ### CREATING TXT FILE ###
    SCRIPT_PATH = os.path.dirname(__file__) #<-- chemin absolue d'où se situe le script
    abs_file_path = os.path.join(SCRIPT_PATH, path)
    #########################
    try:
        df = dd.read_csv(abs_file_path, dtype={'CustomerID': 'str',
       'InvoiceNo': 'str'}, encoding="ISO-8859-1")
    except:
        print("Fichier introuvable.")
        exit()
    df.to_parquet(f'{path}.parquet')

def get_article_names(path):
    """ Fonction permettant de retourner le nom de tous les articles dans 
        notre fichier excel, sous forme de tableau.
    return: (tab[str])
    path: str(path_oh_the_file)
    """
    df = dd.read_parquet(f'{path}.parquet', columns=['StockCode'])
    tab = [] # tableau pour stocker la référence de chaque article
    for data in df['StockCode']:
        if str(data) not in tab:
            tab += [str(data)]
    return tab

def get_orders(path):
    """ Fonction permettant de récupérer toutes les commandes et les articles de ces commandes
        dans un dictionnaire.
    return: (dict)[str(id_order)][list(str(article_names))]
    path: str(path_of_the_file)
    """
    df = dd.read_parquet(f'{path}.parquet', columns=['InvoiceNo','StockCode'])
    orders = {} # dict où stocker les commandes
    for v in df.itertuples():
        row = [v.InvoiceNo, v.StockCode]
        if row[0] not in orders:
            orders[row[0]] = []
            orders[row[0]] += [row[1]]
        else:
            orders[row[0]] += [row[1]]
    return orders

def create_data_frame(names, orders):
    """ Fonction permettant de créer un tableau par rapport aux nom des articles
        et au contenu des commandes.
    return: (list) matrice de résultat
    """
    l1 = names.copy()
    l1.insert(0, "id_articles")
    tab = [l1]
    for name in names:
        tab += [[name]]
    taille = len(names)
    i = 0
    for row in tab:
        if row == tab[0]:
            pass
        else:
            y = 0
            while y < taille:
                if i == y+1:
                    row += ['NA']
                else:
                    row += [0]
                y += 1
        i += 1
    for order in orders:
        for article in orders[order]:
            index = names.index(article) + 1
            indexs = []
            for a in orders[order]:
                if a != article:
                    indexs += [names.index(a) + 1]
            for ind in indexs:
                tab[index][ind] += 1
    return tab

def write_csv(tab):
    """ Fonction permettant de créer et d'écrire le csv des résultat.
    tab: (list) matrice de résultat
    """
    f = open('data/resultat.csv', 'w+')
    for row in tab:
        for col in row:
            f.write(f"{col},")
        f.write("\n")
    f.close()

### Programme Principal ###
if __name__ == "__main__":
    path = ask_path()
    start = time.time()
    open_file(path)
    step1 = time.time()
    print("Conversion des donnees en .parquet :",(step1-start),"sec")
    names = get_article_names(path)
    step2 = time.time()
    print("Recuperation du nom des articles :",(step2-step1),"sec")
    orders = get_orders(path)
    step3 = time.time()
    print("Recuperation des commandes et leurs contenu :",(step3-step2),"sec")
    tab = create_data_frame(names, orders)
    step4 = time.time()
    print("Creation de la matrice de resultat :",(step4-step3),"sec")
    write_csv(tab)
    step5 = time.time()
    print("Creation et ecriture du fichier de resultat :",(step5-step4),"sec")
    end = time.time()
    print("Temps d'execution total du programme :",(end-start),"sec")
