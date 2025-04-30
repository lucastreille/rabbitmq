# Projet RabbitMQ - Système de calcul distribué

**Projet réalisé par**
Lucas TREILLE
Asaad Mehidi
Gabriel Changrenier

Ce projet implémente un système de calcul distribué en utilisant RabbitMQ comme middleware de messagerie. Il permet d'effectuer des opérations mathématiques de manière asynchrone et distribuée à travers différents composants communicant via des exchanges et des files d'attente.

## Architecture du système

Le système est composé de quatre éléments principaux :

1. **Client émetteur** : Génère et envoie des requêtes de calcul au système via des exchanges
2. **Serveur RabbitMQ** : Gère les exchanges, les files d'attente et le routage des messages
3. **Workers** : Traitent les requêtes, effectuent les calculs et renvoient les résultats
4. **Client récepteur** : Affiche les résultats des calculs

## Fonctionnalités

Le projet implémente progressivement des fonctionnalités de plus en plus avancées :

### Phase initiale
- Client émetteur qui envoie des requêtes d'addition toutes les 5 secondes
- Worker générique qui traite les additions avec un délai de 5-15 secondes
- Client récepteur qui affiche les résultats formatés

### Amélioration 1 : Workers spécialisés
- 4 types de workers spécialisés: addition, soustraction, multiplication et division
- Client émetteur amélioré qui envoie des requêtes de différents types aléatoirement
- Les requêtes sont envoyées à des intervalles aléatoires entre 1 et 5 secondes

### Amélioration 2 : Opération "all"
- Possibilité d'envoyer une requête qui sera traitée par les 4 types de workers
- Tous les résultats sont affichés par le client récepteur avec une indication spéciale

### Extension implémentée : Client utilisateur interactif
- Interface en ligne de commande permettant à l'utilisateur d'envoyer ses propres calculs
- Support de toutes les opérations (add, sub, mul, div, all)
- Validation des entrées pour éviter les erreurs

## Architecture avec RabbitMQ Exchanges

Notre implémentation utilise des exchanges RabbitMQ pour un routage intelligent des messages :

1. **Exchange direct `task_exchange`** : Pour router les requêtes d'addition basiques
2. **Exchange direct `op_exchange`** : Pour router les requêtes vers les workers spécialisés selon l'opération
3. **Exchange fanout `all`** : Pour diffuser une même requête à tous les types de workers
4. **Exchange direct `result_exchange`** : Pour centraliser les résultats des calculs

Ce design basé sur les exchanges offre plusieurs avantages :
- Meilleure séparation des responsabilités (routage vs stockage)
- Diffusion simplifiée des requêtes "all" via l'exchange fanout
- Architecture plus évolutive et maintenable

## Prérequis

- Node.js (v14 ou supérieur)
- npm
- Accès à un serveur RabbitMQ (dans notre cas: `amqp://user:password@infoexpertise.hopto.org:5676`)

## Installation

1. Cloner le dépôt :
```bash
git clone https://github.com/votre-depot/projet-rabbitmq.git
cd projet-rabbitmq
```

2. Installer les dépendances :
```bash
npm install
```

3. La configuration de l'accès à RabbitMQ est déjà définie dans `src/config.js`.

## Structure du projet

```
projet-rabbitmq/
├── package.json         # Configuration du projet et scripts npm
├── src/
│   ├── config.js           # Configuration commune (URL RabbitMQ, exchanges, queues)
│   ├── client-sender.js    # Client qui envoie des requêtes automatiquement
│   ├── client-receiver.js  # Client qui affiche les résultats
│   ├── worker.js           # Worker qui effectue les calculs
│   └── user-client.js      # Client interactif pour l'utilisateur
└── README.md            # Documentation du projet
```

## Utilisation

Chaque composant du système peut être lancé séparément dans des terminaux différents.

### Cas d'utilisation 1 : Phase initiale (additions simples)

1. **Terminal 1** - Démarrer le client récepteur :
```bash
npm run start:receiver
```

2. **Terminal 2** - Démarrer un worker générique :
```bash
npm run start:worker
```

3. **Terminal 3** - Démarrer le client émetteur en mode basique :
```bash
npm run start:sender:basic
```
ou simplement (mode basique par défaut) :
```bash
npm run start:sender
```

Vous verrez des requêtes d'addition envoyées toutes les 5 secondes via l'exchange `task_exchange`, traitées par le worker, et les résultats affichés par le client récepteur.

### Cas d'utilisation 2 : Workers spécialisés

1. **Terminal 1** - Démarrer le client récepteur :
```bash
npm run start:receiver
```

2. **Terminaux 2-5** - Démarrer les workers spécialisés (un ou plusieurs dans chaque terminal) :
```bash
npm run start:worker:add   # Dans le terminal 2
npm run start:worker:sub   # Dans le terminal 3
npm run start:worker:mul   # Dans le terminal 4
npm run start:worker:div   # Dans le terminal 5
```

3. **Terminal 6** - Démarrer le client émetteur en mode opérations variées :
```bash
npm run start:sender:ops
```

Vous verrez des requêtes de différents types (add, sub, mul, div) envoyées aléatoirement via l'exchange `op_exchange`, chacune routée vers le worker spécialisé correspondant grâce aux clés de routage.

### Cas d'utilisation 3 : Opération "all"

1. **Terminal 1** - Démarrer le client récepteur :
```bash
npm run start:receiver
```

2. **Terminaux 2-5** - Démarrer les workers spécialisés (un ou plusieurs dans chaque terminal) :
```bash
npm run start:worker:add   # Dans le terminal 2
npm run start:worker:sub   # Dans le terminal 3
npm run start:worker:mul   # Dans le terminal 4
npm run start:worker:div   # Dans le terminal 5
```

3. **Terminal 6** - Démarrer le client émetteur en mode incluant "all" :
```bash
npm run start:sender:all
```

Vous verrez des requêtes de différents types, y compris des requêtes "all" qui sont diffusées via l'exchange fanout `all` à tous les workers simultanément, produisant 4 résultats différents pour les mêmes opérandes.

### Cas d'utilisation 4 : Client interactif

1. **Terminal 1** - Démarrer le client récepteur :
```bash
npm run start:receiver
```

2. **Terminaux 2-5** - Démarrer les workers spécialisés (un dans chaque terminal) :
```bash
npm run start:worker:add   # Dans le terminal 2
npm run start:worker:sub   # Dans le terminal 3
npm run start:worker:mul   # Dans le terminal 4
npm run start:worker:div   # Dans le terminal 5
```

3. **Terminal 6** - Envoyer des requêtes spécifiques :
```bash
npm run calc 10 5 add   # Addition: 10 + 5
npm run calc 20 4 sub   # Soustraction: 20 - 4
npm run calc 6 7 mul    # Multiplication: 6 * 7
npm run calc 100 5 div  # Division: 100 / 5
npm run calc 12 3 all   # Toutes les opérations sur 12 et 3
```

Les résultats sont affichés dans le premier terminal par le client récepteur.

## Commandes disponibles

Le projet offre plusieurs scripts npm pour lancer chaque composant individuellement:

- `npm run start:sender` : Démarre le client émetteur en mode basique par défaut
- `npm run start:sender:basic` : Démarre le client émetteur en mode basique (additions uniquement)
- `npm run start:sender:ops` : Démarre le client émetteur en mode opérations variées
- `npm run start:sender:all` : Démarre le client émetteur en mode incluant l'opération "all"
- `npm run start:receiver` : Démarre le client récepteur qui affiche les résultats
- `npm run start:worker` : Démarre un worker générique (mode initial)
- `npm run start:worker:add` : Démarre un worker spécialisé pour l'addition
- `npm run start:worker:sub` : Démarre un worker spécialisé pour la soustraction
- `npm run start:worker:mul` : Démarre un worker spécialisé pour la multiplication
- `npm run start:worker:div` : Démarre un worker spécialisé pour la division
- `npm run calc [n1] [n2] [op]` : Exécute un calcul spécifique avec le client interactif

## Points techniques importants

### Exchanges et Files d'attente

Le système utilise une architecture sophistiquée basée sur des exchanges RabbitMQ:

**Exchanges:**
- `task_exchange` (Direct) : Route les requêtes basiques vers la queue des tâches génériques
- `op_exchange` (Direct) : Route les requêtes spécifiques vers les queues spécialisées selon l'opération demandée
- `all` (Fanout) : Diffuse les requêtes "all" à toutes les queues spécialisées simultanément
- `result_exchange` (Direct) : Centralise tous les résultats vers la queue de résultats

**Files d'attente:**
- `task_queue` : Pour les tâches en mode initial
- `add_queue`, `sub_queue`, `mul_queue`, `div_queue` : Pour les opérations spécialisées
- `result_queue` : Pour les résultats des calculs

### Routage des messages

- Les messages envoyés à `op_exchange` sont routés selon leur clé de routage (add/sub/mul/div)
- Les messages envoyés à l'exchange fanout `all` sont automatiquement distribués à toutes les queues liées
- Les résultats sont envoyés à `result_exchange` avec des clés de routage 'result' ou 'all-result'

### Résilience et persistance

- Les messages sont marqués comme `persistent: true` pour assurer leur durabilité en cas de redémarrage de RabbitMQ
- Les workers utilisent `prefetch(1)` pour ne traiter qu'un message à la fois
- Les messages sont acquittés (`ack`) après traitement complet pour éviter la perte de messages

### Simulation de calculs complexes

- Les workers simulent des calculs complexes avec un délai aléatoire entre 5 et 15 secondes
- Cette temporisation permet d'observer le comportement asynchrone et distribué du système

## Respect des exigences du projet

### Phase initiale
- Implémentation d'un client qui envoie des requêtes de calcul toutes les 5 secondes
- Format de requête JSON: `{ "n1": 5, "n2": 3 }`
- Worker qui effectue l'addition des opérandes avec un délai simulé
- Format de résultat JSON: `{ "n1": 5, "n2": 3, "op": "add", "result": 8 }`
- Client récepteur qui lit et affiche les résultats

### Amélioration 1
- Implémentation des 4 catégories de workers: add, sub, mul, div
- Paramétrage des workers à leur démarrage
- Client émetteur qui envoie des requêtes variées aléatoirement
- Intervalle entre les requêtes réduit et variable

### Amélioration 2
- Implémentation de l'opération "all" qui envoie la requête aux 4 types de workers
- Client émetteur capable d'envoyer des requêtes "all" aléatoirement

### Extension implémentée
- Client interactif permettant à l'utilisateur d'envoyer ses propres calculs

## Dépannage

### Problèmes de connexion RabbitMQ
- Vérifiez que l'URL du serveur RabbitMQ est correcte dans `config.js`
- Assurez-vous que le serveur RabbitMQ est accessible depuis votre environnement

### Requêtes sans réponse
- Vérifiez que tous les composants sont en cours d'exécution (client récepteur, workers)
- Assurez-vous que les déclarations d'exchanges et les bindings sont cohérents entre les différents composants
- Pour les opérations "all", vérifiez que le client récepteur est bien lié aux deux clés de routage : 'result' et 'all-result'
