import os
#########################################################
## Programme d'installation des librairies nécessaire  ##
# /!\ Nécessite d'avoir python 3.9.7 ajouté au PATH /!\ #
#########################################################

print("Debut de l'installation...")

os.system("python -m pip install dask[dataframe]")
os.system("python -m pip install pyarrow")

print("Installation des librairies terminees.")