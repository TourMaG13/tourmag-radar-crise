# Radar Crise Moyen-Orient — TourMaG

Tableau de bord automatisé qui agrège les articles TourMaG sur la crise au Moyen-Orient, les catégorise par thématique et y ajoute des données financières en temps réel.

## Architecture

```
Flux RSS TourMaG ──→ GitHub Actions (cron 2h) ──→ Firebase Firestore ──→ Frontend (GitHub Pages)
API Yahoo Finance ─┘
```

## Setup

### 1. Créer le projet Firebase

1. Aller sur [console.firebase.google.com](https://console.firebase.google.com)
2. Cliquer **Ajouter un projet** → nommer par ex. `tourmag-radar-crise`
3. Désactiver Google Analytics (pas utile ici)
4. Une fois créé, aller dans **Firestore Database** → **Créer une base de données**
5. Choisir **Mode production** et la région `eur3 (europe-west)`
6. Copier-coller les rules depuis `firestore.rules` dans l'onglet Règles

### 2. Générer la clé de service account

1. Dans Firebase Console → **Paramètres du projet** (roue dentée) → **Comptes de service**
2. Cliquer **Générer une nouvelle clé privée**
3. Un fichier JSON est téléchargé — c'est la clé d'accès au projet

### 3. Configurer le repo GitHub

1. Créer un nouveau repo (public) sur GitHub, par ex. `tourmag-radar-crise`
2. Pousser tout le contenu de ce dossier dans le repo
3. Aller dans **Settings** → **Secrets and variables** → **Actions**
4. Créer un secret :
   - Nom : `FIREBASE_SERVICE_ACCOUNT`
   - Valeur : le **contenu intégral** du fichier JSON téléchargé à l'étape 2 (copier-coller tout le JSON)
5. (Optionnel) Dans l'onglet **Variables**, créer :
   - `RSS_URL` → l'URL du flux RSS si elle change
   - `CONFLICT_START_DATE` → date de début du conflit au format YYYY-MM-DD (défaut : 2025-10-01)

### 4. Tester

1. Aller dans l'onglet **Actions** du repo
2. Sélectionner le workflow **Sync Radar Crise**
3. Cliquer **Run workflow** → **Run workflow**
4. Vérifier les logs : vous devriez voir les articles parsés, catégorisés et écrits dans Firestore
5. Vérifier dans la console Firebase que les collections `articles`, `market_data` et `config` sont peuplées

### 5. Le cron tourne tout seul

Le workflow se déclenche automatiquement toutes les 2 heures. Chaque exécution :
- Parse le flux RSS et détecte les nouveaux articles
- Les catégorise par thématique via mots-clés
- Met à jour les cours de bourse et matières premières
- Écrit tout dans Firestore

## Structure Firestore

### Collection `articles`
```
articles/{id}
  ├── title: string
  ├── link: string
  ├── description: string
  ├── image_url: string
  ├── author: string
  ├── pub_date: timestamp
  ├── category: string (institutionnel|rapatriement|aerien|croisiere|temoignages|experts|juridique|solutions|general)
  ├── countries: array<string> (israel, liban, iran, jordanie...)
  └── created_at: timestamp
```

### Collection `market_data`
```
market_data/{symbol_key}
  ├── symbol: string (ticker Yahoo Finance)
  ├── label: string (nom affiché)
  ├── currency: string (€ ou $)
  ├── sector: string (aerien, hotellerie, croisiere, ota, tech, commodity, forex)
  ├── current_price: number
  ├── start_price: number (au début du conflit)
  ├── change_pct: number (variation en %)
  ├── history: array<{date, close}>
  └── last_update: string (ISO)
```

### Document `config/radar`
```
config/radar
  ├── last_sync: string (ISO)
  ├── conflict_start_date: string
  ├── rss_url: string
  └── last_new_articles: number
```

## Catégorisation par mots-clés

Le fichier `keywords.json` contient les mots-clés pour chaque thématique. L'algorithme :
1. Concatène titre + description de l'article
2. Compte le nombre de mots-clés matchés par thématique
3. Assigne la thématique avec le score le plus élevé
4. Si aucun match → catégorie `general`

Pour affiner la catégorisation, éditer `keywords.json` et relancer le workflow.

## Données financières

Symboles Yahoo Finance utilisés :
- `BZ=F` — Brent Crude Oil
- `EURUSD=X` — Parité EUR/USD
- `AF.PA` — Air France-KLM
- `TUI1.DE` — TUI Group
- `AC.PA` — Accor
- `BKNG` — Booking Holdings
- `CCL` — Carnival Corp
- `AMS.MC` — Amadeus IT
- `AIR.PA` — Airbus
- `RYA.IR` — Ryanair

Pour ajouter/supprimer des valeurs, modifier le dict `FINANCE_SYMBOLS` dans `sync_radar.py`.
