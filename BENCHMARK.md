# ⚡ Benchmark — MushMC BedWars ETL Pipeline

> Resultados reais de execução do pipeline sobre o arquivo `latest.log` original.
> Ambiente: Python 3.x · PySpark 3.x · local[*] · Ubuntu

---

## 📊 Métricas de Execução

| Métrica | Resultado |
|---|---|
| Arquivo de entrada | `latest.log` (Windows-1252) |
| Tamanho do arquivo | **40,82 KB** |
| Total de linhas no log | **500 linhas** |
| Linhas de CHAT extraídas | **277 linhas** |
| Eventos classificados | **212 eventos** |
| Partidas identificadas | **2 partidas** |
| Encoding Win-1252 → UTF-8 | **2,19 ms** |
| Ingestão + Filtro Spark | **6,34 s** |
| **Tempo total de processamento** | **~6,34 s** |

---

## 📁 Distribuição de Eventos Processados

| Categoria | Eventos |
|---|---|
| Sistema | 104 |
| PvP | 31 |
| Início de Partida | 15 |
| Economia/Compra | 15 |
| Sala/Lobby | 14 |
| Cama Destruída | 10 |
| Time Eliminado | 9 |
| Progressão/XP | 7 |
| Economia/Coleta | 5 |
| Chat/Toxicidade | 2 |
| **Total** | **212** |

---

## 🗂️ Eventos por Partida

| Partida | Eventos |
|---|---|
| Pré-jogo / Lobby (match_id = 0) | 83 |
| Partida 1 — 13:01:07 → 13:01:49 (match_id = 1) | 34 |
| Partida 2 — 13:02:04 → 13:06:22 (match_id = 2) | 95 |

---

## 🏗️ Ambiente de Teste

```
Sistema Operacional : Ubuntu (Linux)
Runtime             : Python 3.x
Framework           : Apache Spark 3.x (PySpark)
Modo                : local[*] — todos os cores disponíveis
Adaptive QE         : Desabilitado (spark.sql.adaptive.enabled = false)
```

---

## 📤 Outputs Gerados

```
data/curated/
├── bedwars_analytics.csv          # 212 eventos estruturados
└── dashboards/
    ├── 01_top_itens.png           # Top 5 itens comprados
    ├── 02_volume_eventos.png      # Volume de eventos por categoria
    ├── 03_top_killers.png         # Top 5 jogadores mais letais
    ├── 04_performance_pvp.png     # K/D ratio pessoal (donut chart)
    └── 05_camas_destruidas.png    # Camas destruídas por time
```

---

## 🔬 Decisões de Engenharia Validadas

**Encoding Windows-1252 → UTF-8 em 2,19 ms**
O pré-processamento fora do Spark normalizou o arquivo inteiro em menos de 3 ms, evitando corrupção de caracteres especiais (`ã`, `ê`, `Í`) na JVM.

**Particionamento de sessões com Window Functions**
`monotonically_increasing_id()` + cumulative sum identificou corretamente as 2 partidas no log sem nenhum marcador externo, apenas pela detecção de `"O jogo iniciou!"`.

**Bypass do Hadoop no Windows**
A exportação via `toPandas().to_csv()` contornou o erro nativo de escrita do Spark em ambientes Windows sem `winutils.exe`, gerando o CSV e os 5 dashboards PNG sem falhas.

---

*Benchmark executado em 27/04/2026 — [Davi Ramos Ferreira](https://github.com/Daviramos7)*
