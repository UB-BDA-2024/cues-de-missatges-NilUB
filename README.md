[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-718a45dd9cf7e7f842a935f5ebbe5719a5e09af4491e668f4dbf3b35d5cca122.svg)](https://classroom.github.com/online_ide?assignment_repo_id=15125374&assignment_repo_type=AssignmentRepo)
## Informe sobre Bases de Dades Avançades

## Bases de Dades Estudiades

### Redis
Redis és una base de dades en memòria, de codi obert, que s'utilitza principalment com a magatzem de dades clau-valor. És coneguda per la seva alta velocitat i el seu suport per a estructures de dades complexes com ara cadenes, llistes, conjunts, mapes i altres. Redis és ideal per a aplicacions que requereixen un accés ràpid a les dades, com ara sistemes de memòria cau, gestió de sessions i missatgeria en temps real.

### MongoDB
MongoDB és una base de dades NoSQL orientada a documents. Emmagatzema dades en format BSON (una extensió del JSON), la qual cosa permet una gran flexibilitat en l'estructura de les dades. MongoDB és molt utilitzat en aplicacions web modernes i mòbils gràcies a la seva capacitat de manejar grans volums de dades no estructurades i a la seva escalabilitat horitzontal.

### Elasticsearch
Elasticsearch és un motor de cerca i anàlisi de text distribuït, basat en Apache Lucene. Permet la indexació i cerca ràpida de grans volums de dades. Elasticsearch és àmpliament utilitzat en aplicacions que necessiten una cerca potent i en temps real, com ara aplicacions web amb funcionalitats de cerca avançades, monitoratge de logs i analítica de dades.

### Timescale
TimescaleDB és una base de dades de sèries temporals, construïda sobre PostgreSQL. Està optimitzada per gestionar i analitzar grans volums de dades temporals. Timescale és ideal per a aplicacions que necessiten emmagatzemar i analitzar dades amb marques de temps, com ara monitoratge de sistemes, dades de sensors IoT i analítica financera.

### Cassandra
Apache Cassandra és una base de dades distribuïda NoSQL dissenyada per gestionar grans volums de dades a través de molts servidors. És coneguda per la seva alta disponibilitat, escalabilitat i tolerància a fallades. Cassandra és utilitzada en aplicacions que requereixen un alt rendiment d'escriptura i lectura, com ara sistemes de recomanació, registres d'activitat i analítica en temps real.

### RabbitMQ
RabbitMQ és un agent de missatgeria de codi obert que implementa el protocol AMQP (Advanced Message Queuing Protocol). S'utilitza per a la interconnexió de components de programari a través de missatges, permetent una comunicació asíncrona i escalable. RabbitMQ és essencial en arquitectures orientades a serveis (SOA) i en sistemes de microserveis.

## Refactorització e Integració

Per a la pràctica final del curs, he dut a terme una refactorització significativa del nostre projecte. L'objectiu principal ha estat combinar tots els tests de les diferents pràctiques anteriors i modificar el codi de les bases de dades per assegurar que totes funcionin correctament entre sí.

Els passos principals de la refactorització han inclòs:

- Unificació de Tests:

He revisat i consolidat tots els tests existents per assegurar-nos que cobreixen tots els aspectes i funcionalitats de cada base de dades.
Els tests s'han unificat en un sol conjunt, permetent una execució més eficient i una validació consistent de tot el sistema.

-Modificació del Codi:

He actualitzat el codi de les bases de dades per garantir que puguin interactuar entre sí sense problemes.
Aquesta modificació ha implicat ajustar les connexions, transformar dades entre diferents formats i assegurar la coherència transaccional.

-Integració i Validació:

He realitzat proves exhaustives per verificar que totes les bases de dades funcionen correctament i que els tests passen amb èxit.
S'han solucionat els possibles conflictes i problemes d'integració detectats durant les proves.
 









