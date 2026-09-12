# Open-Source-Datenintegrationsplattform

## 1. Zielsetzung

Die Plattform soll Daten aus heterogenen Quellsystemen zuverlässig aufnehmen, zentral speichern, verarbeiten und für Analysen sowie Anwendungen bereitstellen. Sie löst insbesondere folgende Probleme:

- uneinheitliche Datenformate und Schnittstellen
- manuelle und schwer reproduzierbare Datenimporte
- fehlende Nachvollziehbarkeit von Datenänderungen
- getrennte Speicher- und Verarbeitungssysteme
- unklare Datenqualität und fehlende Verantwortlichkeiten
- Abhängigkeit von proprietären Plattformen und Diensten

Alle eingesetzten Komponenten müssen als FOSS unter einer anerkannten freien Open-Source-Lizenz verfügbar sein. Die Architektur soll Batch-, inkrementelle, CDC- und Streaming-Verarbeitung unterstützen, ohne einzelne Hersteller oder Cloud-Dienste vorauszusetzen.

## 2. Grundlagen

### 2.1 Lakehouse

Das Lakehouse verbindet die kostengünstige Speicherung eines Data Lakes mit den Verwaltungs- und Transaktionseigenschaften eines Data Warehouses. Daten werden in offenen Formaten gespeichert und von mehreren Engines verarbeitet.

### 2.2 Datenebenen

- **Bronze:** Rohdaten in möglichst unveränderter Form einschließlich Quellinformationen, Ladezeitpunkt und technischer Metadaten
- **Silver:** Typisierte, bereinigte, deduplizierte und fachlich vereinheitlichte Daten
- **Gold:** Aggregierte Datenmodelle, Fakten, Dimensionen und Data Marts für konkrete Anwendungsfälle

### 2.3 Batch, inkrementelle Verarbeitung und CDC

- **Batch:** periodische Verarbeitung vollständiger oder definierter Datenmengen
- **Inkrementell:** Verarbeitung ausschließlich neuer oder geänderter Datensätze
- **CDC:** Erfassung von Änderungen direkt aus dem Transaktionslog einer Datenbank
- **Streaming:** kontinuierliche Verarbeitung ereignisbasierter Daten mit geringer Latenz

### 2.4 Offene Datenstandards

- **Parquet:** spaltenorientiertes Format für analytische Daten
- **Avro:** schemaorientiertes Format für Ereignisse und Nachrichten
- **JSON und CSV:** Austausch- und Rohdatenformate
- **Apache Iceberg:** offenes Tabellenformat mit ACID-Transaktionen, Snapshots, Time Travel sowie Schema- und Partitionsevolution

## 3. Erforderliche Komponenten

Die Plattform benötigt:

1. Ingestion für APIs, Dateien, Datenbanken und Ereignisquellen
2. S3-kompatiblen Objektspeicher
3. offenes Tabellenformat
4. zentralen Katalog
5. Transformations- und Verarbeitungsengines
6. SQL-Abfrageebene
7. BI- und Reporting-Werkzeug
8. Workflow-Orchestrierung
9. Datenqualitätsprüfungen
10. Metadaten-, Lineage- und Governance-Funktionen
11. Identitäts-, Rechte- und Secrets-Verwaltung
12. Monitoring, Logging und Alerting

## 4. Optionen und Vergleich

### 4.1 Ingestion

| Option                              | Geeignet für                               | Stärken                                                      | Einschränkungen                                                    |
| ----------------------------------- | ------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------------ |
| **Meltano**                         | APIs, Dateien, Datenbanken und Singer-Taps | FOSS, CLI- und pipelineorientiert, gut automatisierbar       | Connectorqualität abhängig vom jeweiligen Tap                      |
| **Apache NiFi**                     | Dateien, APIs, Protokolle und Routing      | grafische Flows, viele Prozessoren, gute Nachvollziehbarkeit | höherer Ressourcenbedarf und komplexerer Betrieb                   |
| **Debezium + Apache Kafka Connect** | CDC aus Datenbanken                        | zuverlässige Log-basierte Erfassung, geringe Latenz          | zusätzlicher Betriebsaufwand                                       |
| **Apache Kafka**                    | Ereignisse und dauerhafte Datenströme      | Pufferung, Entkopplung, hohe Skalierbarkeit                  | für einfache Batch-Importe überdimensioniert                       |
| **Eigene Skripte**                  | kleine, spezielle Importe                  | maximale Flexibilität, geringer Einstieg                     | Wartung, Tests und Wiederholbarkeit müssen selbst umgesetzt werden |

Für standardisierte Quellen ist Meltano die schlanke Standardlösung. NiFi eignet sich für komplexe technische Flows. Debezium und Kafka werden ergänzt, sobald CDC oder Streaming mit hoher Ereignisrate benötigt wird.

### 4.2 Speicher, Dateiformate und Tabellenformat

| Komponente     | Optionen                                     | Bewertung                                                                                                                                                     |
| -------------- | -------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Objektspeicher | RustFS, Ceph, SeaweedFS, Garage              | RustFS ist für eine einfache S3-kompatible Architektur geeignet; Ceph bietet die umfassendste verteilte Speicherplattform, benötigt aber mehr Betriebsaufwand |
| Dateiformate   | Parquet, Avro, JSON, CSV                     | Parquet für analytische Tabellen, Avro für Events, JSON und CSV primär für Rohdaten und Austausch                                                             |
| Tabellenformat | Apache Iceberg                               | beste Passung für Multi-Engine-Zugriff, ACID, Snapshots und Schemaevolution                                                                                   |
| Katalog        | Apache Polaris, Project Nessie, JDBC-Catalog | Polaris ist der bevorzugte REST-Katalog für Iceberg; Nessie ist interessant, wenn versionierte Katalogoperationen benötigt werden                             |

Apache Iceberg bildet die zentrale Abstraktion zwischen Speicher und Verarbeitung. Dadurch bleiben Daten unabhängig von einer einzelnen Abfrage- oder Transformationsengine.

### 4.3 Transformation und Verarbeitung

| Option           | Einsatz                                      | Stärken                                                                       | Einschränkungen                                                |
| ---------------- | -------------------------------------------- | ----------------------------------------------------------------------------- | -------------------------------------------------------------- |
| **dbt Core**     | SQL-Modelle, Tests und Dokumentation         | FOSS, modular, verständliche Abhängigkeiten, gute Eignung für Silver und Gold | nicht für beliebige Streaminglogik geeignet                    |
| **SQLMesh**      | SQL-Transformationen und Modellversionierung | FOSS, planbasierte Ausführung, gute Kontrolle über Änderungen                 | kleineres Ökosystem als dbt                                    |
| **Apache Spark** | große Batch-Jobs und Backfills               | sehr skalierbar, ausgereiftes Ökosystem                                       | höherer Ressourcen- und Betriebsaufwand                        |
| **Apache Flink** | Streaming, CDC und Event-Time-Verarbeitung   | Stateful Processing, Windowing und niedrige Latenz                            | komplexer Betrieb und höhere Anforderungen an die Modellierung |

dbt Core oder SQLMesh sollte für reguläre SQL-Modelle verwendet werden. Spark wird nur für große Batch-Verarbeitungen ergänzt. Flink kommt hinzu, wenn kontinuierliche Verarbeitung oder komplexe CDC-Logik erforderlich ist.

### 4.4 Abfrage und BI

| Komponente          | Funktion                                                | Geeignet für                                        |
| ------------------- | ------------------------------------------------------- | --------------------------------------------------- |
| **Trino**           | verteilte SQL-Abfragen über Iceberg und weitere Quellen | zentrale Abfrageebene und BI                        |
| **DuckDB**          | lokale SQL-Analysen und Tests                           | Entwicklung, Datenprüfung und kleine Auswertungen   |
| **Apache Superset** | Dashboards und Reporting                                | interaktive BI und Self-Service-Analysen            |
| **ClickHouse**      | hochperformante analytische Abfragen                    | sehr hohe Abfrageparallelität oder niedrige Latenz  |
| **Apache Druid**    | zeitbasierte Ereignis- und OLAP-Abfragen                | Echtzeit-Dashboards und Zeitreihen                  |
| **Apache Pinot**    | interaktive Echtzeitanalysen                            | sehr niedrige Latenz bei großen Ereignisdatenmengen |

Trino, DuckDB und Superset bilden die Standardausstattung. Ein zusätzliches Serving-System ist nur erforderlich, wenn messbare Anforderungen an Latenz, Abfragevolumen oder Echtzeitfähigkeit nicht mit Trino erfüllt werden.

### 4.5 Orchestrierung, Qualität und Governance

| Bereich               | Optionen                              | Empfehlung                                                                                                          |
| --------------------- | ------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| Orchestrierung        | Apache Airflow, Apache Dagster        | Airflow für etablierte, umfangreiche Workflows; Dagster für stärker datenorientierte Pipelines                      |
| Datenqualität         | dbt-Tests, Great Expectations, Deequ  | dbt-Tests als Standard; Great Expectations oder Deequ für zusätzliche fachliche und statistische Prüfungen          |
| Katalog und Lineage   | OpenMetadata, OpenLineage             | OpenMetadata als zentraler Katalog, OpenLineage für systemübergreifende technische Lineage                          |
| Identität und Zugriff | Keycloak, OpenLDAP, OPA               | Keycloak für OIDC und SSO, OpenLDAP bei bestehender LDAP-Infrastruktur, OPA für zentralisierte Autorisierungsregeln |
| Secrets               | OpenBao                               | zentrale Verwaltung und Rotation von Zugangsdaten                                                                   |
| Monitoring            | Prometheus, Grafana, Loki, OpenSearch | Prometheus und Grafana für Metriken und Dashboards; Loki für Logs, OpenSearch für Suche und längere Aufbewahrung    |

## 5. Empfohlene Architektur

### 5.1 Standardarchitektur

```text
Quellen
  ├─ APIs, Dateien, Datenbanken
  ├─ CDC-Quellen
  └─ Event-Streams
        │
        ▼
Ingestion
  ├─ Meltano
  ├─ Apache NiFi bei komplexen Flows
  └─ Debezium + Kafka bei CDC und Streaming
        │
        ▼
Lakehouse
  ├─ RustFS als S3-kompatibler Objektspeicher
  ├─ Parquet als Dateiformat
  ├─ Apache Iceberg als Tabellenformat
  └─ Apache Polaris als REST-Katalog
        │
        ▼
Verarbeitung
  ├─ dbt Core oder SQLMesh für SQL-Modelle
  ├─ Apache Spark für große Batch-Jobs
  └─ Apache Flink für kontinuierliche Verarbeitung
        │
        ▼
Zugriff
  ├─ Trino für zentrale SQL-Abfragen
  ├─ DuckDB für lokale Analysen
  └─ Apache Superset für BI und Reporting
```

Querschnittsfunktionen:

```text
Apache Airflow
  → Orchestrierung und Zeitplanung

OpenMetadata + OpenLineage
  → Katalog, Dokumentation und Lineage

Keycloak + OPA + OpenBao
  → Identität, Autorisierung und Secrets

Prometheus + Grafana + Loki
  → Metriken, Dashboards, Logs und Alerts
```

### 5.2 Datenfluss

```text
Quelle
  → Ingestion
  → Bronze: Rohdaten und technische Metadaten
  → Silver: bereinigt, typisiert und dedupliziert
  → Gold: fachliche Modelle und Data Marts
  → Trino
  → Superset oder weitere Anwendungen
```

Jeder Verarbeitungsschritt muss idempotent oder über einen reproduzierbaren Snapshot erneut ausführbar sein. Alle Tabellen benötigen mindestens Ladezeitpunkt, Quellsystem, Batch- oder Ereignis-ID sowie eine definierte Aufbewahrungs- und Fehlerstrategie.

## 6. Erweiterungen

### 6.1 Zusätzliche CDC-Anforderungen

Wenn Datenbanken nahezu in Echtzeit repliziert werden sollen:

- Debezium und Kafka Connect ergänzen
- Apache Kafka als dauerhaften Ereignispuffer einsetzen
- Flink oder Spark Structured Streaming für die Verarbeitung verwenden
- Schlüssel, Reihenfolge, Duplikate und verspätete Ereignisse explizit behandeln
- Bronze-Daten unverändert und mit Offset beziehungsweise Ereignis-ID speichern

### 6.2 Zusätzliche Echtzeitanforderungen

Wenn interaktive Abfragen im Millisekundenbereich erforderlich sind:

- ClickHouse, Apache Druid oder Apache Pinot ergänzen
- Gold-Daten oder ausgewählte Ereignisse zusätzlich materialisieren
- Trino weiterhin als Lakehouse-Abfrageebene verwenden
- Aktualisierungsintervall, Ziel-Latenz und zulässige Datenverzögerung messbar definieren

### 6.3 Zusätzliche Governance- und Compliance-Anforderungen

Wenn personenbezogene oder besonders schützenswerte Daten verarbeitet werden:

- Keycloak für zentrale Identitäten und Single Sign-on einsetzen
- OPA für feingranulare Autorisierungsregeln ergänzen
- OpenBao für Secrets und Schlüsselmaterial verwenden
- Tabellen- und Spaltenklassifikationen in OpenMetadata pflegen
- Maskierung, Pseudonymisierung und Löschkonzepte als eigene Transformationen modellieren
- Audit-Logs und Zugriffsereignisse zentral speichern und überwachen

### 6.4 Höhere Verfügbarkeits- und Skalierungsanforderungen

Wenn die Plattform ohne einzelne Ausfallpunkte betrieben werden muss:

- RustFS, Kafka, Katalog, Orchestrierung und Abfrageebene redundant auslegen
- Metadaten- und Konfigurationsspeicher auf hochverfügbare PostgreSQL-Instanzen stützen
- Backups, Wiederherstellung und regelmäßige Ausfalltests einplanen
- Zielwerte für Verfügbarkeit, Wiederherstellungszeit und maximalen Datenverlust definieren

## 7. Ergebnis

Die empfohlene Plattform besteht aus einem offenen Lakehouse-Kern mit RustFS, Parquet, Apache Iceberg und Apache Polaris. Meltano deckt standardisierte Ingestion ab; NiFi, Debezium, Kafka, Spark und Flink werden nur bei entsprechenden technischen Anforderungen ergänzt. dbt Core oder SQLMesh bilden die Standardebene für SQL-Transformationen. Trino, DuckDB und Apache Superset stellen die Daten für Abfragen, Entwicklung und BI bereit.

Airflow, OpenMetadata, OpenLineage, Keycloak, OPA, OpenBao sowie Prometheus, Grafana und Loki vervollständigen Orchestrierung, Governance und Betrieb. Dadurch bleibt die Plattform modular, reproduzierbar, multi-engine-fähig und vollständig auf FOSS-Komponenten aufgebaut.

---

# Open-Source-Datenintegrationsplattform

## 1. Zielsetzung

Die Plattform soll in ihrer Grundausführung Daten aus APIs, Dateien und Datenbanken aufnehmen, speichern, verarbeiten und für SQL-Abfragen sowie BI bereitstellen. Perspektivisch soll es um die Verarbeitung von CDC und Ereignisquellen erweitert werden können.

Sie soll:

- unterschiedliche Datenquellen einheitlich integrieren,
- Rohdaten nachvollziehbar erhalten,
- Daten reproduzierbar verarbeiten,
- Datenqualität prüfen,
- mindestens einfache Batch-Verarbeitung ermöglichen,
- bei Bedarf um CDC und Event-Streaming erweitert werden,
- ausschließlich aus FOSS-Komponenten bestehen.

## 2. Grundlagen

Ein **Lakehouse** ist ein Architekturkonzept für analytische Datenverarbeitung. Es verbindet einen Objektspeicher (Data Lake) mit Ebenen für Tabellenverwaltung, Metadaten, Verarbeitung und Abfragen.

Daten können nach ihrem Verarbeitungszustand unterschieden werden:

- **Rohdaten:** unverändert übernommene Daten
- **Bereinigte Daten:** geprüfte, typisierte und vereinheitlichte Daten
- **Fachliche Datenmodelle:** für Berichte und Anwendungen aufbereitete Daten

Diese Zustände werden nach den Medallion-Architekturmuster als Bronze, Silver und Gold bezeichnet.

Ein **Dateiformat** legt die Struktur einzelner Dateien fest. Ein **Tabellenformat** beschreibt, wie mehrere Dateien als zusammengehörige, versionierte Tabelle verwaltet werden. Ein **Katalog** verwaltet die Metadaten dieser Tabellen.

**Batch-Verarbeitung** verarbeitet Daten gesammelt zu festgelegten Zeitpunkten. **CDC (Change Data Management)** erfasst Änderungen in Datenbanken. **Event-Streaming** verarbeitet Ereignisse fortlaufend und mit geringer Verzögerung.

## 3. Benötigte Komponenten

Die Plattform benötigt:

1. Datenübernahme (Ingestion)
2. Objektspeicher
3. Dateiformat
4. Tabellenformat
5. Katalog
6. Transformation
7. SQL-Abfrage (Querying)
8. BI
9. Orchestrierung
10. Datenqualität
11. Governance
12. Monitoring

Für CDC und Event-Streaming kommen zusätzlich Komponenten für Ereignistransport und kontinuierliche Verarbeitung hinzu.

## 4. Optionen und Vergleich

### 4.1 Datenübernahme

| Option         | Geeignet für                        | Bewertung                                                           |
| -------------- | ----------------------------------- | ------------------------------------------------------------------- |
| Airbyte        | APIs, SaaS, Dateien und Datenbanken | viele Konnektoren, geeignet für Standardquellen                     |
| Apache NiFi    | komplexe Datenflüsse                | flexibel, aber höherer Betriebsaufwand                              |
| Eigene Skripte | einzelne Sonderquellen              | flexibel, aber hoher Eigenaufwand                                   |
| Debezium       | CDC aus Datenbanken                 | zuverlässige Änderungsübernahme, nicht für allgemeine Batch-Imports |

### 4.2 Ereignistransport

| Option        | Geeignet für                                            | Bewertung                                                        |
| ------------- | ------------------------------------------------------- | ---------------------------------------------------------------- |
| Apache Kafka  | hohe Ereignismengen, dauerhafte Streams und Entkopplung | skalierbar, aber beträchtlicher Betriebsaufwand                  |
| Apache Pulsar | Streams mit mehreren Mandanten und langer Aufbewahrung  | leistungsfähig, aber komplexer und kleineres Ökosystem           |
| NATS          | leichte Ereignisübertragung mit niedriger Latenz        | einfacher, aber weniger für umfangreiche Datenpipelines geeignet |

Für CDC und umfangreiche Event-Streams ist Apache Kafka die geeignetste Standardoption. Für einfache Batch-Pipelines wird kein Ereignistransport benötigt.

### 4.3 Objektspeicher

| Option    | Bewertung                                                            |
| --------- | -------------------------------------------------------------------- |
| RustFS    | geringer Betriebsaufwand und S3-Kompatibilität                       |
| Ceph      | hohe Skalierbarkeit und Redundanz, aber komplexer Betrieb            |
| SeaweedFS | verteilte Speicherung bei moderatem Betriebsaufwand                  |
| Garage    | leichtgewichtig, aber weniger für sehr große Installationen geeignet |

### 4.4 Dateiformat

| Option  | Geeignet für                             |
| ------- | ---------------------------------------- |
| Parquet | analytische Daten und Tabellen           |
| JSON    | flexible Rohdaten und API-Antworten      |
| CSV     | einfache Importe und Exporte             |
| Avro    | strukturierte Ereignisse und Nachrichten |

### 4.5 Tabellenformat

| Option         | Bewertung                                                                                     |
| -------------- | --------------------------------------------------------------------------------------------- |
| Apache Iceberg | breite Engine-Unterstützung, ACID, Snapshots und Schemaevolution                              |
| Apache Hudi    | gute Unterstützung für Upserts und inkrementelle Verarbeitung                                 |
| Delta Lake     | ausgereifte Transaktionsfunktionen, außerhalb bestimmter Umgebungen teilweise eingeschränkter |

### 4.6 Katalog

| Option         | Bewertung                            |
| -------------- | ------------------------------------ |
| Apache Polaris | offener Katalog für Iceberg          |
| Project Nessie | versionierbare Katalogänderungen     |
| JDBC-Katalog   | einfach, aber mit weniger Funktionen |

### 4.7 Transformation

| Option       | Geeignet für                                            |
| ------------ | ------------------------------------------------------- |
| dbt Core     | SQL-Modelle, Tests und Dokumentation                    |
| Apache Spark | große Batch-Verarbeitungen und Backfills                |
| Apache Flink | kontinuierliche Verarbeitung und komplexe Ereignislogik |

### 4.8 Abfrage

| Option     | Geeignet für                                           |
| ---------- | ------------------------------------------------------ |
| Trino      | zentrale SQL-Abfragen über Lakehouse-Tabellen          |
| DuckDB     | lokale Analysen und Tests                              |
| ClickHouse | besonders niedrige Abfragelatenz und hohe Parallelität |

### 4.9 BI

| Option          | Geeignet für                                  |
| --------------- | --------------------------------------------- |
| Apache Superset | Dashboards, Reports und SQL-basierte Analysen |
| Metabase        | einfache Self-Service-Analysen                |
| Grafana         | operative Dashboards und Zeitreihen           |

### 4.10 Orchestrierung

| Option           | Bewertung                                                   |
| ---------------- | ----------------------------------------------------------- |
| Apache Airflow   | umfangreich, etabliert und für abhängige Workflows geeignet |
| Apache Dagster   | stark auf datenbezogene Abhängigkeiten ausgerichtet         |
| Cron und Skripte | nur für einfache Abläufe geeignet                           |

### 4.11 Datenqualität

| Option             | Geeignet für                                |
| ------------------ | ------------------------------------------- |
| dbt-Tests          | grundlegende Prüfungen in Datenmodellen     |
| Great Expectations | umfangreiche fachliche Prüfungen            |
| Deequ              | statistische Prüfungen großer Datenbestände |

## 5. Empfohlene Architektur und Tech-Stack

Für die erste Ausbaustufe mit einfachen Batch-Imports wird folgende Architektur empfohlen:

- Airbyte für die Datenübernahme
- RustFS als Objektspeicher
- Parquet als Dateiformat
- Apache Iceberg als Tabellenformat
- Apache Polaris als Katalog
- dbt Core für Transformationen und Tests
- Trino für SQL-Abfragen
- Apache Superset für BI
- Apache Airflow für die Orchestrierung

## 6. Erweiterungen

### 6.1 CDC

Für CDC wird die Batch-Ingestion um Debezium und Apache Kafka erweitert.
Die Tabellen müssen zusätzlich Änderungsart, Änderungszeitpunkt und Ereignis-ID speichern. Die Transformationen müssen Duplikate, Löschungen und verspätete Ereignisse behandeln.

### 6.2 Event-Streaming

Für externe Ereignisquellen wird Apache Kafka als Eingangsschicht ergänzt.
Flink übernimmt kontinuierliche Verarbeitung, Zustandsverwaltung und Windowing. Die bestehende Batch-Verarbeitung mit Airbyte und dbt bleibt für nicht ereignisbasierte Quellen bestehen.

### 6.3 Große Batch-Verarbeitung

Wenn dbt Core für große Joins oder Backfills nicht ausreicht, wird Apache Spark ergänzt. Speicher, Tabellenformat und Katalog bleiben unverändert.

### 6.4 Höhere Abfragegeschwindigkeit

Wenn Trino die erforderliche Abfragegeschwindigkeit nicht erreicht, wird ClickHouse ergänzt. Ausgewählte Gold-Daten werden zusätzlich dorthin übertragen; Iceberg bleibt das führende Tabellen- und Speichersystem.

### 6.5 Governance und Monitoring

Bei höheren Anforderungen werden OpenMetadata, OpenLineage, Keycloak, OpenBao, Prometheus, Grafana und Loki ergänzt. Diese Komponenten erweitern Katalog, Lineage, Zugriffskontrolle, Secrets, Metriken und Logs, ohne die grundlegende Datenarchitektur zu ersetzen.
