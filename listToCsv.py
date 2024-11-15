import csv
from collections import Counter


# Generate a csv compatible with the format of https://wordcloud.online/fr

text: str = "pièce de bureau, trouver, nom, domiciliation, adresse, logement, pubs, sites, rémunération, morsure, chat, pharmacie, motivation, travailler, chômage, médicaments, carte vitale, pharmacie, boncoin, livraison, voiture, mutuelle, opération, remboursement, Trump, angoisse, future, iphone, ios, mise à jour, propriétaire, malhonnête, carte d'identité, véganisme, exploitation, animaux, PsyLab, vulgarisation, psychologie, demande, greffe, déposer, Colis, La Poste, douane, ingénieur, logiciel embarqué, chômage, dédicace, BD, timide, Outlook, mot de passe, code de sécurité, ustensiles, mineurs, magasin, journal, impartialité, droits de l'homme, True crime, Adaptation cinématographique, Acteur principal, étude, France, universités, intersection, dépassement, accident, frère, sœur, château, serveurs, clients, touristes, accents, France, dialect, vendeurs, courtoisie, clients, permis de conduire, traduction, extrait de naissance, tabagisme passif, appartement, filtre, éducateur spécialisé, vieux, mandats, carte son, casque, enregistrement, mur, dommage, choc, femme, travail, uestionnaires injustice, Pôle Emploi, CV, expérience professionnelle, contrat de travail, SNCF, processus de recrutement, cartoon, dog, French, Tablette, Utilisation, Taille, modèle classe c, batterie, véhicule, France, espérance de vie, maladies, Jura, randonnées, dégustation, Reddit, commentaires, problème, France, region, department, , commu Reddit, françaises, parc, véhicules, gyrophare, période d'essai, délai de prévenance, licenciement, sub francophone, r/pettyrevenge, r/traumatizethemback, perdre du poids, conseils, surpoids, sardine, conserves, marque, formation, sécurité informatique, certifications, dictionnaire, etymologique, langue, streaming, site, recenser, nom de famille, originaire, France, marque, normal, connaissez, petites poitrines, conseils, communauté, SACEM, adhésion, remboursement, stations de ski, train, Marseille, stations de ski, train, Marseille, stations de ski, train, Marseille, lampe torche, qualité, rechargeable,"

text_list = text.split("," )

# text_list = [s.strip() for s in text_list if s.strip()]

print(text_list)

# Count occurrences of each string
occurrences = Counter(text_list)

# Specify the CSV file name
csv_file = 'output.csv'

# Write to CSV
with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)

    sorted_occurrences = sorted(occurrences.items(), key=lambda item: item[1], reverse=False)
    
    for string, count in occurrences.items():
        writer.writerow([string, count])

print(f"Data written to {csv_file}")