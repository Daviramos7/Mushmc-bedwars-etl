```markdown
# 🕹️ MushMC BedWars ETL Pipeline

> Engenharia de Dados · PySpark · Python · Pandas · DataViz

[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)]()
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)]()
[![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)]()

---

## 👋 Sobre

Este projeto demonstra a arquitetura e construção de um Pipeline ETL ponta a ponta (End-to-End) para processamento de logs brutos e não estruturados.

O objetivo é ingerir arquivos legados (Client-Side) gerados pelo servidor de BedWars *MushMC* (Minecraft 1.8.9), transformá-los e gerar tabelas analíticas e dashboards visuais de forma totalmente automatizada.

---

## 🚀 Stack Principal

`Apache Spark (PySpark)` `Python` `Pandas` `Matplotlib` `Seaborn` `Regex`

---

## 🧠 Desafios de Engenharia Resolvidos

- **Resolução de Encoding Legado (Windows-1252):** O sistema fonte gera logs em codificação legada que o Spark corrompe por padrão (UTF-8). Implementou-se um pré-processamento em Python puro para normalizar os dados antes da ingestão na JVM.
- **Parsing Complexo com Regex:** Motor de expressões regulares para varrer linhas caóticas de chat e categorizar eventos (Economia, PvP, Objetivos), extraindo agressores, vítimas e recursos de forma estruturada.
- **Particionamento Sequencial de Sessões:** Como o log acumula múltiplas partidas no mesmo arquivo, foram utilizadas **Window Functions** (`monotonically_increasing_id` + cumulative sum) para criar IDs de partição cronológica (`match_id`).
- **Bypass do Hadoop no Windows:** Para evitar o erro nativo do Spark ao escrever dados (`winutils.exe` / `NullPointerException`) em ambientes locais, a exportação foi delegada ao Pandas, garantindo a gravação limpa do arquivo `.csv` e a geração das imagens (PNG).

---

## ▶️ Como Executar e Testar

A lógica analítica do código é universal para qualquer partida de BedWars no servidor MushMC. No entanto, por ser um log Client-Side, o jogo registra ações locais em primeira pessoa (ex: "Você comprou Lã").

Para que o pipeline reconheça o seu personagem, siga as diretrizes abaixo:

**1. Clone o repositório:**
```bash
git clone https://github.com/Daviramos7/Mushmc-bedwars-etl.git
```

**2. Instale as dependências:**
```bash
pip install -r requirements.txt
```

**3. Adicione seus dados:**

Coloque o seu arquivo `latest.log` original dentro da pasta `data/raw/` (crie as pastas, caso não existam).

**4. Configure seu Nickname *(Passo Crítico)*:**

Abra o arquivo `src/mushmc_bedwars_etl.py` e, na linha 27, altere o valor da constante `JOGADOR_PRINCIPAL` para o seu nick exato do Minecraft:

```python
JOGADOR_PRINCIPAL = "seu_nick_aqui"
```

**5. Execute o pipeline:**

A partir da raiz do projeto, rode o comando:

```bash
python src/mushmc_bedwars_etl.py
```

---

## 📊 Outputs Gerados

Após a execução bem-sucedida, a pasta `data/curated/` será criada com os seguintes artefatos:

- **Tabela Estruturada:** `bedwars_analytics.csv` (pronta para consumo no Power BI/Excel).
- **Dashboards Visuais:** Uma subpasta `dashboards/` contendo gráficos gerados e estilizados automaticamente pelo Matplotlib/Seaborn (Top Itens, Volume de Eventos, Top Killers, Performance PvP e Camas Destruídas).

---

## 📄 Licença

Copyright © 2026 por Davi Ramos Ferreira. Todos os Direitos Reservados.

Desenvolvido com 💙 por **Davi Ramos Ferreira**
```
