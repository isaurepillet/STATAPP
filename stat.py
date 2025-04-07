import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

#Calcule la moyenne, l'écart-type, et la variance d'une variable et affiche un tableau avec ces valeurs.

def afficher_statistiques(variable):
    # Calculs statistiques
    moyenne = np.mean(variable)
    ecart_type = np.std(variable, ddof=1)  # ddof=1 pour un échantillon
    variance = np.var(variable, ddof=1)

    # Création d'un tableau avec Pandas
    statistiques = {
        'Statistique': ['Moyenne', 'Écart-type', 'Variance'],
        'Valeur': [moyenne, ecart_type, variance]
    }
    df = pd.DataFrame(statistiques)

    # Affichage du tableau
    print("\nTableau des statistiques :\n")
    print(df)

#Trace un graphique pour une variable donnée avec un titre et des labels personnalisés pour les axes.
def tracer_graphique(variable, titre_graphique="Graphique", label_x="Index", label_y="Valeur"):
    # Création du graphique
    plt.figure(figsize=(8, 5))
    plt.plot(variable, marker='o', linestyle='-', label='Données')
    
    # Ajout de la ligne de légende
    plt.title(titre_graphique)
    plt.xlabel(label_x)
    plt.ylabel(label_y)
    plt.legend()
    plt.grid(True)
    plt.show()