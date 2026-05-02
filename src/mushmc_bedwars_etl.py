"""
╔══════════════════════════════════════════════════════════════════════════════╗
║         PIPELINE ETL — MushMC BedWars Log Analyzer                         ║
║         Autor   : dadoscomdavi                                               ║
║         Stack   : PySpark 3.x (sem Pandas, sem UDFs desnecessárias)         ║
║         Versão  : 1.0                                                        ║
║                                                                              ║
║  DESCOBERTAS DO LOG REAL (latest.log):                                       ║
║  • Encoding   : Windows-1252 (Minecraft 1.8.9 no Windows)                   ║
║  • 2 Partidas : Match 1 (13:01:07→13:01:49) | Match 2 (13:02:04→13:06:22)  ║
║  • Padrões extras: PRIMEIRA KILL FINAL!, mortes void sem killer,            ║
║                    XP mid-game vs fim, artefatos de cor nos chats           ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import tempfile
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType
from pyspark.sql.window import Window

# ─────────────────────────────────────────────────────────────────────────────
# INICIALIZAÇÃO
# ─────────────────────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("MushMC_BedWars_ETL")
    .master("local[*]")
    # Desativa o Adaptive Query Execution para logs pequenos (evita overhead
    # de re-planejamento que não traz ganhos em datasets tão pequenos)
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTE DE NEGÓCIO
# ─────────────────────────────────────────────────────────────────────────────
# Centralizar o nome do jogador principal facilita trocar o log de outro
# jogador sem alterar a lógica do pipeline.
JOGADOR_PRINCIPAL = "dadoscomdavi"

# Caminho do arquivo de log (ajuste conforme seu ambiente)
LOG_PATH = "data/raw/latest.log"


# ══════════════════════════════════════════════════════════════════════════════
# PRÉ-PROCESSAMENTO: NORMALIZAÇÃO DE ENCODING (FORA DO SPARK)
# ══════════════════════════════════════════════════════════════════════════════
# Em local mode, spark.read.text() ignora silenciosamente a opção "encoding"
# e usa UTF-8 por padrão, corrompendo os bytes do Windows-1252 (0xEA=ê,
# 0xCD=Í, 0xBB=», 0xE3=ã, etc.). Padrão de produção: decodificar FORA do
# Spark com Python e gravar um artefato UTF-8 temporário para o Spark
# consumir. Em produção, este step seria um operador separado no Airflow.
_utf8_temp = tempfile.mktemp(suffix="_mushmc_utf8.log")
with open(LOG_PATH, "r", encoding="windows-1252", errors="replace") as _src:
    _raw_content = _src.read()
with open(_utf8_temp, "w", encoding="utf-8") as _dst:
    _dst.write(_raw_content)



# ══════════════════════════════════════════════════════════════════════════════
# PASSO 1 — INGESTÃO E FILTRAGEM BASE
# ══════════════════════════════════════════════════════════════════════════════
# ┌─ DECISÃO DE ENGENHARIA ─────────────────────────────────────────────────┐
# │  O arquivo latest.log usa codificação Windows-1252 (charset padrão do   │
# │  Minecraft 1.8.9 em sistemas Windows). Sem especificar a codificação,  │
# │  caracteres como Í (\xcd), ê (\xea), ã (\xe3), » (\xbb) são           │
# │  corrompidos, tornando impossível matchear strings como                 │
# │  "CAMA DESTRUÍDA", "Você", "não" nos passos seguintes.                 │
# │                                                                          │
# │  Usamos spark.read.text() em vez de .csv() ou .json() porque o log     │
# │  é texto não-estruturado — cada linha é a unidade de processamento.    │
# └─────────────────────────────────────────────────────────────────────────┘
df_raw = spark.read.text(_utf8_temp)

# Filtragem: mantemos apenas linhas que contêm [CHAT].
# Esta é uma operação lazy no DAG do Spark — nenhum dado é materializado
# até que uma Action (show, write, count) seja chamada.
df_chat = df_raw.filter(F.col("value").contains("[CHAT]"))


# ══════════════════════════════════════════════════════════════════════════════
# PASSO 2 — EXTRAÇÃO BASE (REGEX)
# ══════════════════════════════════════════════════════════════════════════════
# ┌─ REGEX: Hora ───────────────────────────────────────────────────────────┐
# │  ^\[(\d{2}:\d{2}:\d{2})\]                                               │
# │   ^                  → ancora ao início da linha                        │
# │   \[  ... \]         → colchetes literais (escapados no regex)          │
# │   (\d{2}:\d{2}:\d{2}) → Grupo 1: captura HH:mm:ss com dígitos exatos   │
# └─────────────────────────────────────────────────────────────────────────┘
df_timestamped = df_chat.withColumn(
    "hora",
    F.regexp_extract("value", r"^\[(\d{2}:\d{2}:\d{2})\]", 1)
)

# ┌─ REGEX: Mensagem Limpa ─────────────────────────────────────────────────┐
# │  \[CHAT\]\s*(.+)$                                                        │
# │   \[CHAT\]  → tag literal [CHAT]                                         │
# │   \s*       → consume zero ou mais espaços após a tag                   │
# │   (.+)$     → Grupo 1: tudo que resta até o fim da linha                │
# │                                                                          │
# │  NOTA: Usamos trim() para remover espaço inicial em mensagens como:     │
# │  "[CHAT]  - A aliança..." (linhas de AVISO começam com espaço)          │
# └─────────────────────────────────────────────────────────────────────────┘
df_base = (
    df_timestamped
    .withColumn(
        "mensagem_limpa",
        F.trim(F.regexp_extract("value", r"\[CHAT\]\s*(.+)$", 1))
    )
    # Remove linhas onde [CHAT] vem seguido apenas de espaço em branco.
    # Estas são separadores visuais do servidor (ex: linhas em branco entre
    # anúncios de CAMA DESTRUÍDA).
    .filter(F.length(F.col("mensagem_limpa")) > 0)
)

# ┌─ MATCH ID: identificação de partidas ───────────────────────────────────┐
# │  O log contém DUAS partidas. Para analytics por partida, precisamos     │
# │  de uma chave de partição. Estratégia:                                  │
# │                                                                          │
# │  1. Preservar ordem original com monotonically_increasing_id()          │
# │     (esta função garante IDs crescentes por partição, suficiente        │
# │      para ordenação dentro de um log sequencial de arquivo único)       │
# │                                                                          │
# │  2. Marcar linhas "O jogo iniciou!" como início de nova partida (1)     │
# │                                                                          │
# │  3. Cumulative SUM sobre a janela ordenada por row_id:                  │
# │     → Antes da 1ª partida:  match_id = 0  (lobby/pré-jogo)             │
# │     → Partida 1:            match_id = 1                                │
# │     → Partida 2:            match_id = 2                                │
# │                                                                          │
# │  ⚠ ALERTA DE ESCALA: Window sem partitionBy processa em driver único.   │
# │    Para logs de produção (GB+), particionar por data antes de aplicar. │
# └─────────────────────────────────────────────────────────────────────────┘
df_with_id = df_base.withColumn("_row_id", F.monotonically_increasing_id())

window_order = Window.orderBy("_row_id")

df_with_match = (
    df_with_id
    .withColumn(
        "_is_game_start",
        (F.col("mensagem_limpa") == "O jogo iniciou!").cast("int")
    )
    .withColumn(
        "match_id",
        F.sum("_is_game_start").over(window_order)
    )
)


# ══════════════════════════════════════════════════════════════════════════════
# PASSO 3 — MOTOR DE REGRAS DE NEGÓCIO
# ══════════════════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────────────────────
# DEFINIÇÃO CENTRALIZADA DOS PADRÕES REGEX
# ─────────────────────────────────────────────────────────────────────────────
# Centralizar os padrões evita repetição e facilita manutenção.
# Todos foram validados linha a linha contra o log real.

# ┌─ PAT_MORTE ─────────────────────────────────────────────────────────────┐
# │  Captura TODAS as variantes de morte em um único padrão:                │
# │                                                                          │
# │  Casos cobertos (confirmados no log real):                              │
# │    "caua1803 morreu no void para THALISSINHO01."         → PvP void     │
# │    "caua1803 morreu para THALISSINHO01. KILL FINAL!"     → Kill Final   │
# │    "__migzh morreu para kekuplays. PRIMEIRA KILL FINAL!" → 1ª Kill      │
# │    "THALISSINHO01 morreu no void."                       → Void s/killer│
# │    "dadoscomdavi morreu para uidyugsyugasy. KILL FINAL!" → Minha morte  │
# │                                                                          │
# │  Anatomia:                                                               │
# │   ^(.+?)          → Grupo 1 [LAZY]: vítima — lazy impede que o          │
# │                      nome "engula" o resto da string                    │
# │   morreu          → literal                                              │
# │   (?:\s+no void)? → "no void" é opcional (não-capturante)              │
# │   (?:\s+para\s+   → "para <agressor>" é totalmente opcional            │
# │     (.+?))?       → Grupo 2 [LAZY]: agressor                            │
# │   \.              → ponto final obrigatório antes do kill flag          │
# │   (?:\s*(          → Grupo 3: flag de kill final (opcional)             │
# │     (?:PRIMEIRA\s+)? → "PRIMEIRA" é opcional (nova descoberta no log)  │
# │     KILL FINAL!))?→ fim                                                  │
# └─────────────────────────────────────────────────────────────────────────┘
PAT_MORTE = r"^(.+?) morreu(?:\s+no void)?(?:\s+para\s+(.+?))?\.(?:\s*((?:PRIMEIRA\s+)?KILL FINAL!))?"

# ┌─ PAT_CAMA ─────────────────────────────────────────────────────────────┐
# │  "CAMA DESTRUÍDA » A Cama do Azul foi destruída por THALISSINHO01."    │
# │                                                                          │
# │  ATENÇÃO: Após decodificação Windows-1252, o arquivo contém:           │
# │    \xcd = Í  →  DESTRUÍDA                                               │
# │    \xed = í  →  destruída                                               │
# │    \xbb = »  →  caractere literal no padrão                            │
# │                                                                          │
# │   [IÍ] e [ií]: fallback se encoding falhar parcialmente em algum       │
# │                  ambiente. Preferir caracteres reais ao ASCII puro.     │
# │   (.+?) → Grupo 1 [cor do time]: "Azul", "Verde", "Ciano", etc.        │
# │   (.+?) → Grupo 2 [agressor]                                            │
# └─────────────────────────────────────────────────────────────────────────┘
PAT_CAMA = r"^CAMA DESTRU[IÍ]DA\s+»\s+A Cama do (.+?) foi destru[ií]da por (.+?)\.$"

# ┌─ PAT_TIME ─────────────────────────────────────────────────────────────┐
# │  "TIME ELIMINADO » O Time Verde foi eliminado."                         │
# │  Grupo 1: cor do time eliminado                                         │
# └─────────────────────────────────────────────────────────────────────────┘
PAT_TIME = r"^TIME ELIMINADO\s+»\s+O Time (.+?) foi eliminado\.$"

# ┌─ PAT_SALA ─────────────────────────────────────────────────────────────┐
# │  "dadoscomdavi entrou na sala (4/8)."                                   │
# │  "Pedro1311MG saiu da sala (7/8)."                                      │
# │  Grupo 1: nome do jogador  Grupo 2: ação  Grupo 3: atual  Grupo 4: max │
# └─────────────────────────────────────────────────────────────────────────┘
PAT_SALA = r"^(.+?) (entrou na sala|saiu da sala) \((\d+)/(\d+)\)\.$"

# ┌─ PAT_COMPRA ────────────────────────────────────────────────────────────┐
# │  "Você comprou Picareta de Madeira (Eficiência I)."                     │
# │  "Você comprou Espada de Ferro."                                        │
# │  Grupo 1: nome completo do item (incluindo qualidade entre parênteses)  │
# │                                                                          │
# │  ⚠ Distinguir de "Você não tem recursos o suficiente." —               │
# │    garantido porque "não tem" não é coberto pelo padrão "comprou".      │
# └─────────────────────────────────────────────────────────────────────────┘
PAT_COMPRA = r"^Você comprou (.+?)\.$"

# ┌─ PAT_COLETA ────────────────────────────────────────────────────────────┐
# │  "+36 Ferros"  |  "+1 Ouro"  |  "+1 Ferro"                             │
# │                                                                          │
# │  (?!XP) → lookahead negativo: impede que "+20 XP" seja capturado aqui. │
# │            Sem isso, a XP mid-game seria classificada como coleta.      │
# │  Grupo 1: quantidade (número)                                            │
# │  Grupo 2: tipo do recurso (Ferros, Ouro, Ferro, Diamante, etc.)         │
# └─────────────────────────────────────────────────────────────────────────┘
PAT_COLETA = r"^\+(\d+)\s+(?!XP)([A-Za-zÀ-ÿ]+)$"

# ┌─ PAT_XP_JOGO ───────────────────────────────────────────────────────────┐
# │  XP gerado durante a partida (ao fazer kill, destruir cama, etc.)       │
# │  "+20 XP (Double XP)"  |  "+100 XP (Double XP)"  |  "+200 XP"          │
# │  Grupo 1: valor numérico do XP                                          │
# └─────────────────────────────────────────────────────────────────────────┘
PAT_XP_JOGO = r"^\+(\d+)\s+XP"

# ┌─ PAT_XP_FIM ────────────────────────────────────────────────────────────┐
# │  XP de encerramento de partida (apenas ao sair da tela de morte/vitória)│
# │  "Você ganhou 380 de XP nessa partida!"                                 │
# │  Grupo 1: XP total da partida                                           │
# └─────────────────────────────────────────────────────────────────────────┘
PAT_XP_FIM = r"^Você ganhou (\d+) de XP nessa partida!$"

# ┌─ PAT_CHAT ──────────────────────────────────────────────────────────────┐
# │  "[B] [2?] Moggar_betas: ciano lx"                                      │
# │                                                                          │
# │  ATENÇÃO: "[2?]" e "[||...]" são artefatos de código de cor do          │
# │  Minecraft que o cliente 1.8.9 não renderizou em texto puro.            │
# │  A regex deve ser tolerante a qualquer conteúdo entre colchetes.        │
# │                                                                          │
# │  \[B\]       → literal: prefixo de chat global                         │
# │  \s+\[.+?\]  → rank (qualquer coisa entre colchetes, lazy)             │
# │  \s+(.+?):   → Grupo 1: nome do jogador antes dos dois-pontos          │
# │  \s+(.+)$    → Grupo 2: conteúdo da mensagem (greedy até o fim)        │
# └─────────────────────────────────────────────────────────────────────────┘
PAT_CHAT = r"^\[B\]\s+\[.+?\]\s+(.+?):\s+(.+)$"


# ─────────────────────────────────────────────────────────────────────────────
# EXTRAÇÃO DE GRUPOS (COLUNAS AUXILIARES PREFIXADAS COM _)
# ─────────────────────────────────────────────────────────────────────────────
# ┌─ DECISÃO DE ENGENHARIA ─────────────────────────────────────────────────┐
# │  Extraímos todos os grupos regex PRIMEIRO em colunas auxiliares,        │
# │  depois construímos as colunas de negócio com when/otherwise sobre      │
# │  essas auxiliares.                                                       │
# │                                                                          │
# │  Por quê? Alternativas e seus problemas:                                │
# │   • UDF Python: serialização JVM↔Python por linha = lento              │
# │   • regexp_extract() dentro de when(): Spark reexecuta o regex para     │
# │     CADA cláusula when — mais caro que extrair uma vez e reutilizar.    │
# │   • regexp_extract() em colunas intermediárias: executado UMA vez no   │
# │     plano físico do Spark, resultado armazenado em memória da partição. │
# └─────────────────────────────────────────────────────────────────────────┘
df_extracted = (
    df_with_match
    # Grupos do padrão de morte (3 grupos de captura)
    .withColumn("_morte_vitima",    F.regexp_extract("mensagem_limpa", PAT_MORTE, 1))
    .withColumn("_morte_agressor",  F.regexp_extract("mensagem_limpa", PAT_MORTE, 2))
    .withColumn("_morte_kill_flag", F.regexp_extract("mensagem_limpa", PAT_MORTE, 3))
    # Grupos da cama destruída (2 grupos)
    .withColumn("_cama_cor",        F.regexp_extract("mensagem_limpa", PAT_CAMA, 1))
    .withColumn("_cama_agressor",   F.regexp_extract("mensagem_limpa", PAT_CAMA, 2))
    # Sala/Lobby (4 grupos)
    .withColumn("_sala_jogador",    F.regexp_extract("mensagem_limpa", PAT_SALA, 1))
    .withColumn("_sala_acao",       F.regexp_extract("mensagem_limpa", PAT_SALA, 2))
    .withColumn("_sala_atual",      F.regexp_extract("mensagem_limpa", PAT_SALA, 3).cast("int"))
    .withColumn("_sala_max",        F.regexp_extract("mensagem_limpa", PAT_SALA, 4).cast("int"))
    # Compra (1 grupo)
    .withColumn("_compra_item",     F.regexp_extract("mensagem_limpa", PAT_COMPRA, 1))
    # Coleta de recurso (2 grupos)
    .withColumn("_coleta_qtd",      F.regexp_extract("mensagem_limpa", PAT_COLETA, 1))
    .withColumn("_coleta_tipo",     F.regexp_extract("mensagem_limpa", PAT_COLETA, 2))
    # XP mid-game (1 grupo)
    .withColumn("_xp_jogo_val",     F.regexp_extract("mensagem_limpa", PAT_XP_JOGO, 1))
    # XP fim de partida (1 grupo)
    .withColumn("_xp_fim_val",      F.regexp_extract("mensagem_limpa", PAT_XP_FIM, 1))
    # Chat (2 grupos)
    .withColumn("_chat_jogador",    F.regexp_extract("mensagem_limpa", PAT_CHAT, 1))
    .withColumn("_chat_msg",        F.regexp_extract("mensagem_limpa", PAT_CHAT, 2))
)


# ─────────────────────────────────────────────────────────────────────────────
# 3a. COLUNA: categoria_evento
# ─────────────────────────────────────────────────────────────────────────────
# ┌─ DECISÃO DE PRIORIDADE ─────────────────────────────────────────────────┐
# │  when/otherwise em Spark avalia condições em ORDEM (como if/elif).      │
# │  Regra: MAIS ESPECÍFICO PRIMEIRO para evitar classificações erradas.    │
# │                                                                          │
# │  Exemplo crítico: "MINIKIU_joga morreu para THALISSINHO01. KILL FINAL!" │
# │    → Deve ser "PvP" (regex PAT_MORTE), NÃO "Sistema"                   │
# │  Exemplo crítico: "+20 XP (Double XP)"                                  │
# │    → Deve ser "Progressão/XP" e NÃO "Economia/Coleta" (o lookahead     │
# │      negativo (?!XP) em PAT_COLETA já previne, mas a ordem reforça).   │
# └─────────────────────────────────────────────────────────────────────────┘
df_categorizado = df_extracted.withColumn(
    "categoria_evento",
    F.when(
        # Início de jogo: mensagem exata de disparo do match
        F.col("mensagem_limpa") == "O jogo iniciou!",
        "Início de Partida"
    ).when(
        # Countdown: "O jogo inicia em N segundo(s)!"
        F.col("mensagem_limpa").rlike(r"^O jogo inicia em \d+"),
        "Início de Partida"
    ).when(
        # Cama destruída (verificar ANTES de PvP — ambos têm nomes de jogadores)
        F.col("mensagem_limpa").rlike(PAT_CAMA),
        "Cama Destruída"
    ).when(
        # Time eliminado
        F.col("mensagem_limpa").rlike(PAT_TIME),
        "Time Eliminado"
    ).when(
        # Mortes PvP — cobre void, normal, kill final, primeira kill final
        F.col("mensagem_limpa").rlike(PAT_MORTE),
        "PvP"
    ).when(
        # Sala/Lobby
        F.col("mensagem_limpa").rlike(PAT_SALA),
        "Sala/Lobby"
    ).when(
        # Compra (ANTES de verificar "Você" genérico para evitar conflitos)
        F.col("mensagem_limpa").rlike(PAT_COMPRA),
        "Economia/Compra"
    ).when(
        # Coleta de recurso (+N Ferros, +N Ouro)
        F.col("mensagem_limpa").rlike(PAT_COLETA),
        "Economia/Coleta"
    ).when(
        # XP mid-game: "+N XP ..."
        F.col("mensagem_limpa").rlike(PAT_XP_JOGO),
        "Progressão/XP"
    ).when(
        # XP fim de partida: "Você ganhou N de XP nessa partida!"
        F.col("mensagem_limpa").rlike(PAT_XP_FIM),
        "Progressão/XP"
    ).when(
        # Chat global
        F.col("mensagem_limpa").rlike(PAT_CHAT),
        "Chat/Toxicidade"
    ).otherwise(
        # Tudo mais: mensagens do servidor, avisos, renascimento, MUSH promos
        "Sistema"
    )
)


# ─────────────────────────────────────────────────────────────────────────────
# 3b. COLUNA: jogador_alvo
# ─────────────────────────────────────────────────────────────────────────────
# Quem morreu (em PvP) OU qual time perdeu a cama.
# Para "Cama Destruída", o "alvo" é o time dono da cama, não um jogador
# específico — representado como "Time <Cor>" para ser filtrável em dashboards.
df_alvo = df_categorizado.withColumn(
    "jogador_alvo",
    F.when(
        F.col("categoria_evento") == "PvP",
        F.col("_morte_vitima")
    ).when(
        F.col("categoria_evento") == "Cama Destruída",
        F.concat(F.lit("Time "), F.col("_cama_cor"))
    ).otherwise(F.lit(None).cast(StringType()))
)


# ─────────────────────────────────────────────────────────────────────────────
# 3c. COLUNA: jogador_agressor_ou_ator
# ─────────────────────────────────────────────────────────────────────────────
# Quem matou, quem destruiu a cama, ou quem comprou/coletou.
# O Minecraft 1.8.9 sempre usa segunda pessoa ("Você") para ações do
# jogador local — por isso hardcodamos JOGADOR_PRINCIPAL nas ações de
# economia, que nunca têm o nome explícito no log.
df_agressor = df_alvo.withColumn(
    "jogador_agressor_ou_ator",
    F.when(
        # PvP com agressor identificado (mortes no void solitárias têm _morte_agressor = "")
        (F.col("categoria_evento") == "PvP") & (F.col("_morte_agressor") != ""),
        F.col("_morte_agressor")
    ).when(
        F.col("categoria_evento") == "Cama Destruída",
        F.col("_cama_agressor")
    ).when(
        # Compras, coletas e XP são sempre do jogador local no log cliente
        F.col("categoria_evento").isin("Economia/Compra", "Economia/Coleta", "Progressão/XP"),
        F.lit(JOGADOR_PRINCIPAL)
    ).when(
        # Sala/Lobby: quem entrou/saiu
        F.col("categoria_evento") == "Sala/Lobby",
        F.col("_sala_jogador")
    ).otherwise(F.lit(None).cast(StringType()))
)


# ─────────────────────────────────────────────────────────────────────────────
# 3d. COLUNA: item_ou_recurso
# ─────────────────────────────────────────────────────────────────────────────
# Coluna polimórfica: conteúdo varia por categoria.
#   Compra     → nome completo do item (ex: "Picareta de Madeira (Eficiência I)")
#   Coleta     → "N x Tipo" (ex: "36 x Ferros")
#   XP mid     → "N XP (mid-game)"
#   XP fim     → "N XP (fim de partida)"
#   Sala/Lobby → "N/Max slots" (ex: "4/8") — dado de contexto útil
df_item = df_agressor.withColumn(
    "item_ou_recurso",
    F.when(
        F.col("categoria_evento") == "Economia/Compra",
        F.col("_compra_item")
    ).when(
        F.col("categoria_evento") == "Economia/Coleta",
        F.concat(F.col("_coleta_qtd"), F.lit(" x "), F.col("_coleta_tipo"))
    ).when(
        (F.col("categoria_evento") == "Progressão/XP") & (F.col("_xp_jogo_val") != ""),
        F.concat(F.col("_xp_jogo_val"), F.lit(" XP (mid-game)"))
    ).when(
        (F.col("categoria_evento") == "Progressão/XP") & (F.col("_xp_fim_val") != ""),
        F.concat(F.col("_xp_fim_val"), F.lit(" XP (fim de partida)"))
    ).when(
        F.col("categoria_evento") == "Sala/Lobby",
        F.concat(F.col("_sala_atual").cast("string"), F.lit("/"), F.col("_sala_max").cast("string"))
    ).otherwise(F.lit(None).cast(StringType()))
)


# ─────────────────────────────────────────────────────────────────────────────
# 3e. COLUNA: is_kill_final
# ─────────────────────────────────────────────────────────────────────────────
# True para:
#   • "KILL FINAL!" → kill que elimina um time sem cama
#   • "PRIMEIRA KILL FINAL!" → kill que destrói cama e elimina ao mesmo tempo
#   • "TIME ELIMINADO" → anúncio de eliminação (separado do kill em alguns casos)
df_kill_flag = df_item.withColumn(
    "is_kill_final",
    F.col("mensagem_limpa").rlike(r"KILL FINAL!|TIME ELIMINADO").cast(BooleanType())
)


# ─────────────────────────────────────────────────────────────────────────────
# 3f. COLUNA: is_minha_acao
# ─────────────────────────────────────────────────────────────────────────────
# True se qualquer condição for verdadeira:
#   1. Jogador principal é o agressor/ator (kills, compras, coletas, destruiu cama)
#   2. Jogador principal é a vítima (morreu)
#   3. Mensagem começa com "Você" (ações diretas do cliente: comprou, renasceu, etc.)
#   4. Mensagem de sala onde o jogador principal aparece como ator
#
# Esta coluna simplifica filtros em dashboards sem exigir lógica adicional.
df_final = df_kill_flag.withColumn(
    "is_minha_acao",
    (
        (F.col("jogador_agressor_ou_ator") == JOGADOR_PRINCIPAL) |
        (F.col("jogador_alvo") == JOGADOR_PRINCIPAL) |
        F.col("mensagem_limpa").rlike(r"^Você") |
        F.col("mensagem_limpa").rlike(r"^dadoscomdavi")
    ).cast(BooleanType())
)


# ══════════════════════════════════════════════════════════════════════════════
# PASSO 4 — SELEÇÃO FINAL E OUTPUT
# ══════════════════════════════════════════════════════════════════════════════
# Descartamos todas as colunas brutas (value) e auxiliares (_*).
# O esquema final é o contrato de dados para consumo downstream
# (Power BI, Metabase, dbt, etc.).

COLUNAS_FINAIS = [
    "match_id",                 # Identificador da partida (0=lobby, 1=match1, 2=match2)
    "hora",                     # Timestamp HH:mm:ss
    "mensagem_limpa",           # Mensagem original limpa (para auditoria/debug)
    "categoria_evento",         # Categoria do evento
    "jogador_alvo",             # Vítima / time alvo
    "jogador_agressor_ou_ator", # Agressor / ator
    "item_ou_recurso",          # Item comprado, recurso coletado, XP ganho
    "is_kill_final",            # Flag: kill/eliminação final
    "is_minha_acao",            # Flag: ação envolve dadoscomdavi
]

df_output = df_final.select(COLUNAS_FINAIS)

# Cache o resultado final para evitar recalcular o plano ao executar
# múltiplas queries analíticas abaixo.
df_output.cache()

# ─────────────────────────────────────────────────────────────────────────────
# OUTPUT PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "═" * 120)
print("  PIPELINE ETL — MushMC BedWars | dadoscomdavi — OUTPUT PRINCIPAL")
print("═" * 120)
df_output.show(200, truncate=False)

# ─────────────────────────────────────────────────────────────────────────────
# QUERIES ANALÍTICAS (BÔNUS)
# Demonstram o valor imediato do dado estruturado para um projeto de analytics
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "─" * 80)
print("  [ANALYTICS 1] Distribuição de Eventos por Categoria e Partida")
print("─" * 80)
df_output.groupBy("match_id", "categoria_evento") \
    .count() \
    .orderBy("match_id", F.desc("count")) \
    .show(50)

print("\n" + "─" * 80)
print("  [ANALYTICS 2] Todas as Mortes PvP (quem matou quem)")
print("─" * 80)
df_output.filter(F.col("categoria_evento") == "PvP") \
    .select("match_id", "hora", "jogador_alvo", "jogador_agressor_ou_ator", "is_kill_final", "mensagem_limpa") \
    .show(50, truncate=False)

print("\n" + "─" * 80)
print(f"  [ANALYTICS 3] Performance de {JOGADOR_PRINCIPAL}: Kills e Mortes")
print("─" * 80)
df_pvp = df_output.filter(F.col("categoria_evento") == "PvP")

kills   = df_pvp.filter(F.col("jogador_agressor_ou_ator") == JOGADOR_PRINCIPAL).count()
mortes  = df_pvp.filter(F.col("jogador_alvo") == JOGADOR_PRINCIPAL).count()
kfinals = df_pvp.filter(
    (F.col("jogador_agressor_ou_ator") == JOGADOR_PRINCIPAL) &
    F.col("is_kill_final")
).count()

print(f"  → Kills totais : {kills}")
print(f"  → Mortes totais: {mortes}")
print(f"  → Kill Finais  : {kfinals}")
print(f"  → K/D Ratio    : {kills / mortes:.2f}" if mortes > 0 else "  → K/D Ratio    : ∞")

print("\n" + "─" * 80)
print("  [ANALYTICS 4] Economia: Todas as Compras de dadoscomdavi")
print("─" * 80)
df_output.filter(F.col("categoria_evento") == "Economia/Compra") \
    .select("match_id", "hora", "item_ou_recurso") \
    .show(50, truncate=False)

print("\n" + "─" * 80)
print("  [ANALYTICS 5] Camas Destruídas — Cronologia")
print("─" * 80)
df_output.filter(F.col("categoria_evento") == "Cama Destruída") \
    .select("match_id", "hora", "jogador_alvo", "jogador_agressor_ou_ator", "mensagem_limpa") \
    .show(50, truncate=False)

print("\n" + "─" * 80)
print("  [ANALYTICS 6] XP Acumulado por Partida (dadoscomdavi)")
print("─" * 80)
df_output.filter(
    (F.col("categoria_evento") == "Progressão/XP") &
    (F.col("is_minha_acao") == True)
).select("match_id", "hora", "item_ou_recurso") \
    .show(50, truncate=False)

print("\n" + "─" * 80)
print("  [ANALYTICS 7] Chat Global — Mensagens")
print("─" * 80)
df_output.filter(F.col("categoria_evento") == "Chat/Toxicidade") \
    .select("match_id", "hora", "mensagem_limpa") \
    .show(50, truncate=False)

print("\n" + "═" * 120)
print("  PIPELINE CONCLUÍDO COM SUCESSO")
print("═" * 120 + "\n")

# ─────────────────────────────────────────────────────────────────────────────
# EXPORTAÇÃO FINAL E DATAVIZ (BYPASS HADOOP/WINDOWS)
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "─" * 80)
print("  [EXPORTAÇÃO E DATAVIZ] Gerando CSV e Dashboards (PNG)...")
print("─" * 80)

import os
import time
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.ticker as ticker

# Cria as pastas locais de destino
os.makedirs("data/curated/dashboards", exist_ok=True)

# 1. EXPORTAÇÃO CSV VIA PANDAS (Artefato do Data Lake)
df_final_pandas = df_output.toPandas()
df_final_pandas.to_csv("data/curated/bedwars_analytics.csv", index=False, encoding='utf-8-sig')
print("  [✓] Tabela CSV gerada com sucesso em: data/curated/bedwars_analytics.csv")

# 2. GERAÇÃO DOS DASHBOARDS EM IMAGEM (MATPLOTLIB + SEABORN)
plt.style.use('dark_background')
sns.set_palette("magma")

# --- Gráfico 1: Itens Mais Comprados ---
df_compras = df_final_pandas[df_final_pandas['categoria_evento'] == 'Economia/Compra']
if not df_compras.empty:
    top_itens = df_compras['item_ou_recurso'].value_counts().head(5).reset_index()
    top_itens.columns = ['item_ou_recurso', 'count']

    plt.figure(figsize=(10, 5))
    sns.barplot(data=top_itens, x='count', y='item_ou_recurso')
    plt.title('Top 5 Itens Mais Comprados na Partida', fontsize=14, fontweight='bold', pad=15)
    plt.xlabel('Quantidade Comprada', fontsize=10)
    plt.ylabel('')
    
    for i, v in enumerate(top_itens['count']):
        plt.text(v + 0.1, i, str(v), color='white', va='center', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('data/curated/dashboards/01_top_itens.png', dpi=300)
    plt.close()
    print("  [✓] Dashboard '01_top_itens.png' gerado com sucesso!")

# --- Gráfico 2: Volume de Eventos ---
df_eventos = df_final_pandas[df_final_pandas['match_id'] > 0]
if not df_eventos.empty:
    vol_eventos = df_eventos['categoria_evento'].value_counts().reset_index()
    vol_eventos.columns = ['categoria_evento', 'count']

    plt.figure(figsize=(10, 5))
    sns.barplot(data=vol_eventos, x='count', y='categoria_evento', palette="viridis")
    plt.title('Volume de Eventos Durante o Jogo', fontsize=14, fontweight='bold', pad=15)
    plt.xlabel('Número de Ocorrências', fontsize=10)
    plt.ylabel('')
    
    plt.tight_layout()
    plt.savefig('data/curated/dashboards/02_volume_eventos.png', dpi=300)
    plt.close()
    print("  [✓] Dashboard '02_volume_eventos.png' gerado com sucesso!")

# --- Gráfico 3: Top 5 Jogadores Mais Letais (Kills) ---
df_pvp = df_final_pandas[df_final_pandas['categoria_evento'] == 'PvP']
if not df_pvp.empty:
    top_killers = df_pvp['jogador_agressor_ou_ator'].value_counts().head(5).reset_index()
    top_killers.columns = ['jogador', 'kills']

    plt.figure(figsize=(10, 5))
    sns.barplot(data=top_killers, x='kills', y='jogador', palette="flare")
    plt.title('Top 5 Jogadores Mais Letais (Kills)', fontsize=14, fontweight='bold', pad=15)
    plt.xlabel('Número de Kills', fontsize=10)
    plt.ylabel('')
    
    plt.gca().xaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    for i, v in enumerate(top_killers['kills']):
        plt.text(v + 0.1, i, str(v), color='white', va='center', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('data/curated/dashboards/03_top_killers.png', dpi=300)
    plt.close()
    print("  [✓] Dashboard '03_top_killers.png' gerado com sucesso!")

# --- Gráfico 4: Performance Pessoal PvP (Donut Chart) ---
kills_minhas = len(df_pvp[df_pvp['jogador_agressor_ou_ator'] == 'dadoscomdavi'])
mortes_minhas = len(df_pvp[df_pvp['jogador_alvo'] == 'dadoscomdavi'])

if kills_minhas > 0 or mortes_minhas > 0:
    plt.figure(figsize=(6, 6))
    plt.pie([kills_minhas, mortes_minhas], labels=['Kills Realizadas', 'Vezes que Morreu'], 
            autopct='%1.1f%%', colors=['#00E676', '#FF1744'], startangle=90, explode=(0.05, 0),
            textprops={'color':"white", 'weight':'bold'})
    
    # Adicionando o círculo central para transformar a Pizza em Donut
    centre_circle = plt.Circle((0,0), 0.70, fc='#000000') # Preto para combinar com dark_background
    fig = plt.gcf()
    fig.gca().add_artist(centre_circle)
    
    plt.title('Performance Pessoal PvP', fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.savefig('data/curated/dashboards/04_performance_pvp.png', dpi=300, facecolor='#000000')
    plt.close()
    print("  [✓] Dashboard '04_performance_pvp.png' gerado com sucesso!")

# --- Gráfico 5: Camas Destruídas por Time ---
df_camas = df_final_pandas[df_final_pandas['categoria_evento'] == 'Cama Destruída']
if not df_camas.empty:
    camas_destruidas = df_camas['jogador_alvo'].value_counts().reset_index()
    camas_destruidas.columns = ['time', 'quantidade']

    plt.figure(figsize=(10, 5))
    sns.barplot(data=camas_destruidas, x='quantidade', y='time', palette="coolwarm")
    plt.title('Camas Destruídas por Time Alvo', fontsize=14, fontweight='bold', pad=15)
    plt.xlabel('Camas Perdidas', fontsize=10)
    plt.ylabel('')
    
    plt.gca().xaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    for i, v in enumerate(camas_destruidas['quantidade']):
        plt.text(v + 0.05, i, str(v), color='white', va='center', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('data/curated/dashboards/05_camas_destruidas.png', dpi=300)
    plt.close()
    print("  [✓] Dashboard '05_camas_destruidas.png' gerado com sucesso!")

print("\n" + "═" * 120)
print("  PIPELINE COMPLETAMENTE FINALIZADO (ETL + DATAVIZ)")
print("═" * 120 + "\n")

# Encerramento Seguro
df_output.unpersist()
time.sleep(1)
spark.stop()