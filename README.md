# IOTestbed2

**_FR_**

Ceci est une experimentation sur l'optimisation des IO. L'idée est de grouper les entrées-sorties élémentaires (ex. `select ... where id = X`) et de les transformer en des entrées-sorties groupées (ex. `select id, ... where id in (X1, X2... Xn)`). Les fonctions exécutant les entrées-sorties sont dites _vectorisées_ ; elles prennent en paramètre un vecteur de valeurs (_X1, X2... Xn_) et fournissent un vecteur de résultats (_(X1, result1), (X2, result2)... (Xn, resultN)_). On dérive de ces fonctions _vectorisées_ des fonctions _simples_ à un seul paramètre renvoyant un seul résultat.

Le regroupement des valeurs à passer en paramètre aux fonctions _vectorisées_ se fait en réordonnant l'exécution du programme à la mode monade libre (_free monad_). On doit donc programmer en style monadique, avec l'API fournie par l'objet `Vectorizable` qui met en œuvre la monade libre `Vectorizable.Script`.

Différents cas de tests montrent l'utilisation de `Vectorizable`, notamment la série des `VesselDatabase_XXX_App` simulant la consolidation d'observations de navires. La règle de consolitation est détaillée dans `import_process.puml`. Elle contient différentes suites de `select` plus ou moins longues suivant les cas, ce qui rend un groupage manuel des données dans des requêtes trop complexes (en fin pour moi).

On peut comparer les performances d'une implémentation naïve de l'algorithme (`VesselDatabase_Classic_App`) à celle des versions monadique en style _flat-map_ (`VesselDatabase_FlatMap_App`) et en style notation _for_ (`VesselDatabase_ForNotation_App`). Enfin, la version utilisant https://github.com/ThoughtWorksInc/each (`VesselDatabase_Each_App`) masque un peu le style monadique en produissant une version plus proche de la version classique, mais elle ne fonctionne pas en Scala 2.11 (bug interne du compilateur Scala) et est mal gérée par IntelliJ IDEA (inférence de type incorrecte dans l'éditeur de texte).

Dans la base de données simulée, chaque entrée-sortie prend 10ms, avec 1ms par éléments constituant la requête (grosso-modo nombre d'éléments dans la clause _where_). Une requête à 1 paramètre prend 11ms, une requête à 10 paramètres prend 20ms. Cela modélise la latence inhérente à chaque requête.

Une alternative au style monadique est d'utiliser des _threads_, non-pas pour le parallelisme, mais pour laisser les calculs en suspend en attendant le regroupement des requêtes et l'obtention des résultats. Un nombre résonnable de thread (une vingtaine) permettrait d'attendre des performances intéressantes.
Un essais d'implémentation est en cours.
