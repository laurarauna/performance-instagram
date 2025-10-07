import requests
import json
import pandas as pd
import re
import os
import logging
import time
import random # Adicionado para jitter
import sys # Adicionado para sys.exit
import pyodbc
from unidecode import unidecode
from datetime import datetime, date, timedelta, timezone
from typing import Optional, Dict, Any, Tuple, List

from dotenv import load_dotenv

_ = load_dotenv()  

# --- Configuração do Logging ---
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Handler para console
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
    logger.addHandler(console_handler)

# Handler para arquivo INFO 
info_log_file = os.getenv('INFOLOG')
if info_log_file:
    try:
        # Verifica se um handler para este arquivo já existe
        if not any(isinstance(h, logging.FileHandler) and h.baseFilename == os.path.abspath(info_log_file) for h in logger.handlers):
            info_file_handler = logging.FileHandler(info_log_file, encoding='utf-8')
            info_file_handler.setLevel(logging.INFO)
            info_file_handler.setFormatter(log_formatter)
            logger.addHandler(info_file_handler)
            logger.info(f"Logging de INFO configurado para o arquivo: {info_log_file}")
    except Exception as e:
        logger.error(f"Erro ao configurar o handler de arquivo INFO ({info_log_file}): {e}")
else:
    logger.warning("Variável de ambiente 'INFOLOG' não definida. Logs de INFO não serão salvos em arquivo.")

# Handler para arquivo ERROR (se definido no .env)
error_log_file = os.getenv('ERRORLOG')
if error_log_file:
    try:
         # Verifica se um handler para este arquivo já existe
        if not any(isinstance(h, logging.FileHandler) and h.baseFilename == os.path.abspath(error_log_file) for h in logger.handlers):
            error_file_handler = logging.FileHandler(error_log_file, encoding='utf-8')
            error_file_handler.setLevel(logging.ERROR)
            error_file_handler.setFormatter(log_formatter)
            logger.addHandler(error_file_handler)
            logger.info(f"Logging de ERROR configurado para o arquivo: {error_log_file}")
    except Exception as e:
        logger.error(f"Erro ao configurar o handler de arquivo ERROR ({error_log_file}): {e}")
else:
    logger.warning("Variável de ambiente 'ERRORLOG' não definida. Logs de ERROR não serão salvos em arquivo.")
# --- Fim da Configuração do Logging ---

# Logs de variáveis de ambiente removidos para reduzir ruído

# --- Constantes Globais ---
GRAPH_API_VERSION = 'v23.0' # Definido globalmente para uso na validação e nas chamadas

# --- Conexão com SQL Server ---
DB_SERVER = 'seu_server'
DB_DATABASE = 'seu_bd '
DB_DRIVER = 'ODBC Driver 18 for SQL Server'
DB_USER = os.getenv("DB_USER", "usuario")
DB_PASS = os.getenv("DB_PASS", "senha")

def get_db_connection():
    """Abre conexão com SQL Server usando as mesmas credenciais do teste.py (Trusted Connection)."""
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_DATABASE};"
        f"UID={DB_USER};"
        f"PWD={DB_PASS};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
        "MultiSubnetFailover=Yes;"
        "Connection Timeout=60;"
    )
    return pyodbc.connect(conn_str, timeout=60)

def upsert_dataframe(conn: pyodbc.Connection, df: pd.DataFrame, table_name: str, key_columns: List[str]):
    """Realiza upsert (MERGE) linha a linha no SQL Server para o DataFrame informado.
    - key_columns: chaves lógicas para match
    """
    if df is None or df.empty:
        logger.info(f"Nada para gravar em {table_name} (DataFrame vazio).")
        return

    # Garante ordem e tipos Python nativos
    df_to_write = df.copy()
    # Substitui NaN/NaT por None
    df_to_write = df_to_write.where(pd.notnull(df_to_write), None)
    # Converte valores do tipo list para string para evitar erro do pyodbc
    for col in df_to_write.columns:
        try:
            if any(isinstance(v, (list, dict)) for v in df_to_write[col].dropna()):
                df_to_write[col] = df_to_write[col].apply(
                    lambda v: ', '.join(map(str, v)) if isinstance(v, list) else (
                        json.dumps(v, ensure_ascii=False) if isinstance(v, dict) else v
                    )
                )
        except Exception:
            pass

    columns = list(df_to_write.columns)
    # Verifica se todas as chaves existem
    for k in key_columns:
        if k not in columns:
            raise KeyError(f"Coluna de chave '{k}' não encontrada no DataFrame para {table_name}.")

    # Monta SQL MERGE com placeholders
    src_select = ', '.join([f"? AS [{c}]" for c in columns])
    merge_on = ' AND '.join([f"t.[{k}] = s.[{k}]" for k in key_columns])
    non_key_cols = [c for c in columns if c not in key_columns]
    update_set = ', '.join([f"t.[{c}] = s.[{c}]" for c in non_key_cols]) if non_key_cols else ''
    insert_cols = ', '.join([f"[{c}]" for c in columns])
    insert_vals = ', '.join([f"s.[{c}]" for c in columns])

    merge_sql = (
        f"MERGE INTO [dbo].[{table_name}] AS t "
        f"USING (SELECT {src_select}) AS s "
        f"ON ({merge_on}) "
        + (f"WHEN MATCHED THEN UPDATE SET {update_set} " if update_set else "")
        + f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});"
    )

    rows = [tuple(row[c] for c in columns) for _, row in df_to_write.iterrows()]
    cur = conn.cursor()
    cur.fast_executemany = True
    try:
        cur.executemany(merge_sql, rows)
        conn.commit()
        logger.info(f"Upsert concluído em {table_name}: {len(rows)} linhas.")
    except Exception as e:
        conn.rollback()
        logger.exception(f"Falha no upsert em {table_name}: {e}")
        raise
    finally:
        cur.close()

def combine_city_data(df_followers: pd.DataFrame, df_engage: pd.DataFrame) -> pd.DataFrame:
    """Unifica seguidores e engajamento por cidade numa única tabela alvo."""
    if (df_followers is None or df_followers.empty) and (df_engage is None or df_engage.empty):
        return pd.DataFrame(columns=['PÁGINA', 'CIDADES', 'SEGUIDORES', 'ENGAJAMENTO', 'DATA DE LEITURA'])

    base_cols = ['PÁGINA', 'CIDADES', 'DATA DE LEITURA']
    df_f = df_followers.copy() if df_followers is not None else pd.DataFrame(columns=base_cols + ['SEGUIDORES'])
    df_e = df_engage.copy() if df_engage is not None else pd.DataFrame(columns=base_cols + ['ENGAJAMENTO'])

    for col in base_cols:
        if col not in df_f.columns:
            df_f[col] = None
        if col not in df_e.columns:
            df_e[col] = None

    if 'SEGUIDORES' not in df_f.columns:
        df_f['SEGUIDORES'] = 0
    if 'ENGAJAMENTO' not in df_e.columns:
        df_e['ENGAJAMENTO'] = 0

    # Converte datas para date
    for d in (df_f, df_e):
        if 'DATA DE LEITURA' in d.columns:
            d['DATA DE LEITURA'] = pd.to_datetime(d['DATA DE LEITURA'], errors='coerce').dt.date

    merged = pd.merge(
        df_f[base_cols + ['SEGUIDORES']],
        df_e[base_cols + ['ENGAJAMENTO']],
        on=base_cols,
        how='outer'
    )
    merged['SEGUIDORES'] = pd.to_numeric(merged.get('SEGUIDORES', 0), errors='coerce').fillna(0).astype(int)
    merged['ENGAJAMENTO'] = pd.to_numeric(merged.get('ENGAJAMENTO', 0), errors='coerce').fillna(0).astype(int)
    return merged[base_cols + ['SEGUIDORES', 'ENGAJAMENTO']]

def get_table_columns(conn: pyodbc.Connection, table_name: str) -> List[str]:
    cur = conn.cursor()
    try:
        cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=?", (table_name,))
        rows = cur.fetchall()
        return [r[0] for r in rows]
    finally:
        cur.close()

def align_df_to_table(conn: pyodbc.Connection, df: pd.DataFrame, table_name: str, key_columns: List[str]) -> pd.DataFrame:
    """Mantém apenas colunas que existem na tabela e garante presença das chaves na tabela e no DF."""
    if df is None or df.empty:
        return df
    table_cols = set(get_table_columns(conn, table_name))
    # Garante que as chaves existem no DF
    for k in key_columns:
        if k not in df.columns:
            raise KeyError(f"Coluna de chave '{k}' não encontrada no DataFrame para {table_name}.")
        if k not in table_cols:
            raise KeyError(f"Coluna de chave '{k}' não encontrada na tabela {table_name}.")
    keep_cols = [c for c in df.columns if c in table_cols]
    # Garante que todas as chaves sejam mantidas
    for k in key_columns:
        if k not in keep_cols:
            keep_cols.append(k)
    return df[keep_cols]

# --- Renomeação de colunas (espaços -> underscores) antes de gravar no DB ---
DB_COL_MAP_COMMON = {
    'DATA DE LEITURA': 'DATA_DE_LEITURA',
    'IDADE DA POSTAGEM': 'IDADE_DA_POSTAGEM',
    'HORÁRIO DA POSTAGEM': 'HORÁRIO_DA_POSTAGEM',
}

DB_COL_MAP_PAGINA = {
    'VISUALIZAÇÕES AO PERFIL': 'VISUALIZAÇÕES_AO_PERFIL',
    'CLIQUES NO LINK': 'CLIQUES_NO_LINK',
    **DB_COL_MAP_COMMON,
}

DB_COL_MAP_IDADE = {
    'FAIXA DE IDADE': 'FAIXA_DE_IDADE',
    **DB_COL_MAP_COMMON,
}

DB_COL_MAP_POSTS = {
    **DB_COL_MAP_COMMON,
}

DB_COL_MAP_HASHTAG = {
    **DB_COL_MAP_COMMON,
}

DB_COL_MAP_CIDADE = {
    **DB_COL_MAP_COMMON,
}

def rename_cols_for_db(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    return df.rename(columns=mapping)

# --- Renomeação inversa (underscores -> espaços) para dados lidos do DB ---
DB_COL_MAP_INVERSE = {
    'DATA_DE_LEITURA': 'DATA DE LEITURA',
    'IDADE_DA_POSTAGEM': 'IDADE DA POSTAGEM',
    'HORÁRIO_DA_POSTAGEM': 'HORÁRIO DA POSTAGEM',
    'VISUALIZAÇÕES_AO_PERFIL': 'VISUALIZAÇÕES AO PERFIL',
    'CLIQUES_NO_LINK': 'CLIQUES NO LINK',
    'FAIXA_DE_IDADE': 'FAIXA DE IDADE',
}

def delete_all_rows(conn: pyodbc.Connection, table_name: str) -> None:
    cur = conn.cursor()
    try:
        cur.execute(f"DELETE FROM [dbo].[{table_name}]")
        conn.commit()
        logger.info(f"Tabela {table_name}: todos os registros foram removidos (sobrescrita total).")
    except Exception as e:
        conn.rollback()
        logger.exception(f"Erro ao remover registros de {table_name}: {e}")
        raise
    finally:
        cur.close()

def has_new_rows(conn: pyodbc.Connection, table_name: str, df: pd.DataFrame, key_col: str = 'id') -> bool:
    """Retorna True se existir alguma linha em df cujo key_col não esteja presente na tabela alvo."""
    if df is None or df.empty:
        return False
    try:
        existing = pd.read_sql(f"SELECT [{key_col}] FROM [dbo].[{table_name}]", conn)
        if existing.empty:
            return True
        existing_ids = set(existing[key_col].astype(str))
        new_ids = set(df[key_col].astype(str))
        diff = new_ids - existing_ids
        return len(diff) > 0
    except Exception as e:
        logger.exception(f"Falha ao verificar novas linhas em {table_name}: {e}. Assumindo que há novas linhas.")
        return True

def can_delete(conn: pyodbc.Connection, table_name: str) -> bool:
    """Verifica permissão de DELETE no objeto dbo.<table_name>."""
    try:
        obj = f"dbo.{table_name}"
        row = pd.read_sql("SELECT HAS_PERMS_BY_NAME(?, 'OBJECT', 'DELETE') AS allowed", conn, params=(obj,)).iloc[0]
        return bool(row['allowed'])
    except Exception as e:
        logger.warning(f"Falha ao verificar permissão de DELETE em {table_name}: {e}. Assumindo que NÃO tem permissão.")
        return False

# Cada token será carregado e validado dentro do loop de contas.
# --- Função de Validação do Token ---
def check_fb_token_validity(token_to_validate: str, page_username: str):
    """Verifica a validade de um token de acesso do Facebook Graph API para uma página específica."""
    if not token_to_validate:
        logger.error(f"O token do Facebook para a página '{page_username}' não foi fornecido para validação. Pulando esta conta.")
        return False # Indica que o token é inválido
    
    url = f"https://graph.facebook.com/{GRAPH_API_VERSION}/debug_token?input_token={token_to_validate}&access_token={token_to_validate}"
    logger.info(f"Validando token para '{page_username}' (versão {GRAPH_API_VERSION})...")
    
    response = None
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        if 'error' in data or not data.get('data', {}).get('is_valid'):
            error_message = data.get('error', {}).get('message', 'Token inválido ou expirado.')
            logger.error(f"Token para '{page_username}' é inválido ou expirado. Detalhe da API: {error_message}. Pulando esta conta.")
            return False

        expires_at = data.get('data', {}).get('expires_at')
        logger.info(f"Token para '{page_username}' validado com sucesso.")
        if expires_at and expires_at > 0:
            expiration_date = datetime.fromtimestamp(expires_at, tz=timezone.utc)
            logger.info(f"  Expira em: {expiration_date.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        else:
            logger.info("  Token não expira ou data de expiração não fornecida.")
        return True

    except requests.exceptions.RequestException as req_err:
        logger.error(f"Erro de conexão ao validar o token para '{page_username}': {req_err}. Pulando esta conta.")
        return False
    except json.JSONDecodeError as json_err:
        response_text = response.text if response and hasattr(response, 'text') else "Resposta não disponível."
        logger.error(f"Erro ao decodificar a resposta JSON da validação do token para '{page_username}'. Erro: {json_err}. Resposta: {response_text[:500]}. Pulando esta conta.")
        return False
    except Exception as e:
        logger.error(f"Erro inesperado durante a validação do token para '{page_username}': {e}. Pulando esta conta.")
        return False

# Constantes antigas de caminho (CSV/Excel) removidas

# --- Constantes de Retry ---
MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 10 
BACKOFF_FACTOR = 2
REQUEST_INTERVAL_SECONDS = 4 
JITTER_AMOUNT = 1 # segundos

# --- Variáveis Globais / Setup ---
hoje = datetime.utcnow()
now = hoje.date()

# --- Funções Auxiliares ---

def remover_acentos(texto):
    """Remove acentos de uma string."""
    if isinstance(texto, str):
        return unidecode(texto) 
    return texto # Retorna o input original se não for string

def to_date_days(year, month, day, actual_date = now) -> int:
    """Calcula a diferença em dias entre uma data e a data atual."""
    try:
        target_date = date(int(year), int(month), int(day))
        delta = actual_date - target_date
        return delta.days
    except ValueError:
        logger.warning(f"Data inválida encontrada: {year}-{month}-{day}")
        return -1 # Indica erro ou data inválida

def remover_stopwords(texto, stopwords_set):
    """Remove stopwords de uma string, usando um set para eficiência."""
    if not isinstance(texto, str):
        return texto
    palavras = texto.split()
    palavras_sem_stopwords = [palavra for palavra in palavras if palavra.lower() not in stopwords_set]
    return ' '.join(palavras_sem_stopwords)

# Lógica antiga de caminho de arquivos removida (não usamos mais CSV/Excel)

# --- Constantes de Rate‐Limit Dinâmico ---
RATE_LIMIT_THRESHOLD = 90       # % de chamadas usadas acima do qual aplicamos backoff extra
MAX_EXTRA_BACKOFF = 60          # segundos máximos de espera adicional

def make_request_with_retry(url: str, params: Dict[str, Any] = None, page_id: str = None) -> Optional[Dict[str, Any]]:
    """
    Faz uma requisição GET à API com lógica de retentativa, backoff exponencial
    e pausa extra baseada no cabeçalho de rate-limit 'X-Business-Use-Case-Usage'.
    """
    response_obj = None
    for attempt in range(MAX_RETRIES):
        try:
            wait_time = REQUEST_INTERVAL_SECONDS + random.uniform(0, JITTER_AMOUNT)
            logger.debug(f"Aguardando {wait_time:.2f}s antes da requisição para {url[:100]}...")
            time.sleep(wait_time)

            response_obj = requests.get(url, params=params)
            try:
                response_obj.raise_for_status()
            except requests.exceptions.HTTPError:
                pass

            # checar X-Business-Use-Case-Usage para aplicar backoff dinâmico ---
            usage_header = response_obj.headers.get('x-business-use-case-usage')
            if usage_header and page_id:
                try:
                    usage_data = json.loads(usage_header)
                    if page_id in usage_data:
                        page_usage_list = usage_data[page_id]
                        # Itera sobre os diferentes tipos de limites para a página
                        for usage_info in page_usage_list:
                            # 1. Verifica se a API está pedindo para esperar
                            time_to_wait = usage_info.get('estimated_time_to_regain_access', 0)
                            if time_to_wait > 0:
                                wait_seconds = time_to_wait * 60 + 5 # Converte minutos para segundos e adiciona margem
                                logger.warning(f"API solicitou espera de {time_to_wait} min para Page ID {page_id}. Aguardando {wait_seconds:.0f}s.")
                                time.sleep(wait_seconds)
                                continue # Pula para o próximo tipo de limite na lista

                            # 2. Verifica proativamente os limites de uso
                            call_pct = usage_info.get('call_count', 0)
                            cpu_pct = usage_info.get('total_cputime', 0)
                            time_pct = usage_info.get('total_time', 0)
                            limit_type = usage_info.get('type', 'N/A')

                            # Pega a maior porcentagem de uso
                            max_usage_pct = max(call_pct, cpu_pct, time_pct)

                            if max_usage_pct >= RATE_LIMIT_THRESHOLD:
                                # Calcula um backoff proporcional ao quão perto estamos de 100%
                                over_threshold = max_usage_pct - RATE_LIMIT_THRESHOLD
                                percentage_to_max = over_threshold / (100 - RATE_LIMIT_THRESHOLD)
                                extra_wait = min(MAX_EXTRA_BACKOFF, percentage_to_max * MAX_EXTRA_BACKOFF)

                                logger.warning(
                                    f"Uso de rate-limit ({limit_type.upper()}) para Page ID {page_id} está em {max_usage_pct}%. "
                                    f"(Calls: {call_pct}%, CPU: {cpu_pct}%, Time: {time_pct}%). "
                                    f"Aplicando backoff extra de {extra_wait:.1f}s..."
                                )
                                time.sleep(extra_wait)
                    else:
                        logger.debug(f"Page ID {page_id} não encontrado no cabeçalho X-Business-Use-Case-Usage.")

                except (json.JSONDecodeError, KeyError, TypeError) as e:
                    logger.warning(f"Falha ao processar X-Business-Use-Case-Usage: {e}. Header: {usage_header[:200]}")


            response_obj.raise_for_status()
            logger.debug(f"GET OK {response_obj.status_code} para {url[:120]}")
            return response_obj.json()

        except requests.exceptions.HTTPError as http_err:
            status_code = http_err.response.status_code if http_err.response else None
            text = http_err.response.text[:500] if http_err.response else ""
            if status_code and 400 <= status_code < 500:
                logger.error(f"Erro HTTP {status_code}: {text}. Abortando.")
                return None
            # 5xx ou retryable
        except requests.exceptions.RequestException as req_err:
            logger.warning(f"RequestException: {req_err}")
        except json.JSONDecodeError as json_err:
            logger.error(f"JSONDecodeError: {json_err}")
            return None
        # backoff exponencial normal
        delay = INITIAL_RETRY_DELAY * (BACKOFF_FACTOR ** attempt)
        logger.info(f"Aguardando {delay:.1f}s antes de nova tentativa...")
        time.sleep(delay)

    logger.error(f"Falha definitiva após {MAX_RETRIES} tentativas.")
    return None


def fetch_media_data(params_base: Dict[str, Any], endpointParams_base: Dict[str, Any]) -> Tuple[Optional[pd.DataFrame], Optional[Dict[str, Any]]]:
    """Busca dados de mídia básicos com paginação e retries, limitado a 500 posts."""
    all_media_data = []
    endpointParams = endpointParams_base.copy()
    max_posts_to_fetch = 500 
    ig_username = params_base.get('ig_username', 'N/A') # Para logs
    page_id = params_base.get('page_id') # Para o rate limiter

    logger.info(f"Iniciando busca de mídias básicas (primeira página, limite {max_posts_to_fetch} posts) para {ig_username}...")
    initial_url = params_base['endpoint_base'] + params_base['instagram_account_id'] + '/media'

    current_params = {
        'fields': endpointParams.get('fields'),
        'limit': endpointParams.get('limit'), # Usa o limite por página da API
        'access_token': params_base['access_token']
    }

    basic_insight = make_request_with_retry(initial_url, params=current_params, page_id=page_id)

    if not basic_insight: # make_request_with_retry já logou o erro detalhado.
        logger.error(f"Falha crítica ao buscar a primeira página de mídias para {ig_username}. A busca de mídia para esta conta será abortada. Saindo.")
        sys.exit(1) # Interrompe o script se a primeira página falhar criticamente

    if 'data' not in basic_insight: # Mesmo que a requisição tenha retornado algo, 'data' é essencial
        logger.error(f"Resposta da API para primeira página de mídias de {ig_username} não contém a chave 'data'. Resposta: {str(basic_insight)[:500]}. A busca de mídia para esta conta será abortada. Saindo.")
        sys.exit(1)


    first_page_data = basic_insight.get('data', [])
    total_after_first = len(first_page_data)
    pct_after_first = (total_after_first / max_posts_to_fetch) * 100 if max_posts_to_fetch else 0
    logger.info(
        f"Recebidos {len(first_page_data)} itens na primeira página para {ig_username} "
        f"({total_after_first}/{max_posts_to_fetch}, {pct_after_first:.1f}%)."
    )
    all_media_data.extend(first_page_data)

    page_count = 1
    max_pages = 20 # Mantém um limite de segurança de páginas
    # Continua enquanto houver próxima página, não atingiu limite de páginas E não atingiu limite de posts
    while ('paging' in basic_insight and 'next' in basic_insight['paging'] and
           page_count < max_pages and len(all_media_data) < max_posts_to_fetch):
        next_url = basic_insight['paging']['next']
        page_count += 1
        collected = len(all_media_data)
        pct_collected = (collected / max_posts_to_fetch) * 100 if max_posts_to_fetch else 0
        logger.info(
            f"Buscando página {page_count} de mídias para {ig_username} "
            f"(coletados {collected}/{max_posts_to_fetch}, {pct_collected:.1f}%)..."
        )

        basic_insight = make_request_with_retry(next_url, page_id=page_id)

        if not basic_insight: # make_request_with_retry já logou o erro
            logger.warning(f"Falha ao buscar página {page_count} de mídias para {ig_username}. Parando paginação para esta conta.")
            break # Não interrompe o script inteiro, mas para de paginar para esta conta

        if 'data' not in basic_insight:
            logger.warning(f"Resposta da API para página {page_count} de mídias de {ig_username} não contém a chave 'data'. Resposta: {str(basic_insight)[:500]}. Parando paginação para esta conta.")
            break

        page_data = basic_insight.get('data', [])
        page_items = len(page_data)
        total_after = len(all_media_data) + page_items
        pct_after = (total_after / max_posts_to_fetch) * 100 if max_posts_to_fetch else 0
        logger.info(
            f"Recebidos {page_items} itens na página {page_count} para {ig_username} "
            f"(total {total_after}/{max_posts_to_fetch}, {pct_after:.1f}%)."
        )
        if not page_data:
             logger.info(f"Página {page_count} vazia para {ig_username}. Parando paginação.")
             break

        # Adiciona apenas os posts necessários para atingir o limite
        needed = max_posts_to_fetch - len(all_media_data)
        all_media_data.extend(page_data[:needed])

        # Pausa
        time.sleep(random.uniform(0.5, 1.5))

        # Verifica se atingiu o limite após adicionar
        if len(all_media_data) >= max_posts_to_fetch:
             logger.info(f"Atingido limite de {max_posts_to_fetch} posts para {ig_username}. Parando paginação de mídia.")
             break

    if page_count >= max_pages:
         logger.warning(f"Atingido limite máximo de {max_pages} páginas para busca de mídia para {ig_username} antes de atingir {max_posts_to_fetch} posts.")

    if not all_media_data:
        logger.warning(f"Nenhum dado de mídia coletado para {ig_username} após busca e paginação. Esta conta pode não ter posts ou houve um problema.")
        # Não interrompe o script, get_data tratará o DataFrame vazio.
        return None, None

    logger.info(f"Total de {len(all_media_data)} mídias básicas coletadas (limite era {max_posts_to_fetch}) para {ig_username}.")
    # Retorna apenas os posts até o limite, caso a última página tenha excedido
    return pd.DataFrame(all_media_data[:max_posts_to_fetch]), basic_insight

def encontrar_hashtags(titulo, hashtags):
    """Encontra hashtags específicas em um texto (case-insensitive)."""
    if not isinstance(titulo, str):
        return []
    if not hashtags: # Adicionado para evitar erro se a lista de hashtags monitoradas estiver vazia
        return []
    escaped_hashtags = [re.escape(h) for h in hashtags]
    if not escaped_hashtags: # Segurança adicional se re.escape resultar em strings vazias
        return []
    pattern = r'#\b(?:%s)\b' % '|'.join(escaped_hashtags) 

    try:
        return re.findall(pattern, titulo, re.IGNORECASE)
    except re.error as e:
        logger.error(f"Erro no regex ao buscar hashtags: {e}. Padrão: {pattern}. Título: {titulo[:50]}...")
        return []

# --- Funções de Extração de Insights ---

def extrai_metricas_insight(params_var: dict, metrics: str, breakdown: str = None) -> Optional[Dict[str, Any]]:
    """Função auxiliar para buscar insights genéricos com retries."""
    url = params_var['endpoint_base'] + params_var['instagram_account_id'] + '/insights'
    page_id = params_var.get('page_id') # Para o rate limiter
    insight_params = {
        'metric': metrics,
        'access_token': params_var['access_token'],
        # Ajusta período e outros params baseado na métrica/breakdown
    }
    if breakdown: # Demográficos geralmente usam lifetime/this_month
        insight_params['period'] = 'lifetime'
        insight_params['metric_type'] = 'total_value'
        insight_params['timeframe'] = 'this_month'
        insight_params['breakdown'] = breakdown
    else: # Métricas de conta/usuário geralmente usam 'day'
        insight_params['period'] = 'day'
        # Adiciona metric_type='total_value' também para métricas não demográficas diárias
        insight_params['metric_type'] = 'total_value'

    logger.debug(f"Buscando insights: {url} com params {insight_params}")
    result = make_request_with_retry(url, params=insight_params, page_id=page_id)

    if not result:
        logger.error(f"Falha ao buscar insights para metrics={metrics}, breakdown={breakdown} para a conta. (URL: {url[:100]})")
        # Não interrompe o script aqui, a função chamadora pode decidir como lidar com o None.
        return None
    if 'error' in result:
        # Adiciona a possibilidade de extrair código de erro e subcódigo se disponíveis
        error_details = result['error']
        error_msg = error_details.get('message', 'Erro desconhecido')
        error_code = error_details.get('code')
        error_subcode = error_details.get('error_subcode')
        logger.error(f"Erro da API ao buscar insights ({metrics}, breakdown={breakdown}): {error_msg} (Code: {error_code}, Subcode: {error_subcode})")
        return None
    if 'data' not in result:
        logger.warning(f"Resposta inesperada da API para insights ({metrics}, breakdown={breakdown}): Chave 'data' ausente. Resposta: {str(result)[:500]}")
        return None

    return result


def extrai_idade(params_var: dict) -> pd.DataFrame:
    """Extrai dados demográficos de idade."""
    logger.info(f"Extraindo dados de idade para {params_var['ig_username']}...")
    metrics = 'engaged_audience_demographics,follower_demographics'
    basic_insight = extrai_metricas_insight(params_var, metrics, breakdown='age')

    df_engage_age_default = pd.DataFrame(columns=['PÁGINA', 'FAIXA DE IDADE', 'ENGAJAMENTO'])
    df_follower_age_default = pd.DataFrame(columns=['SEGUIDORES', 'DATA DE LEITURA'])
    df_age_default = pd.concat([df_engage_age_default, df_follower_age_default], axis=1)

    if not basic_insight:
        logger.warning(f"Não foi possível obter insights de idade para {params_var['ig_username']}. Retornando DataFrame de idade padrão.")
        return df_age_default

    try:
        engage_data = next((item for item in basic_insight['data'] if item['name'] == 'engaged_audience_demographics'), None)
        engage_results = []
        # Adiciona verificação se breakdowns existe e não está vazio
        if engage_data and 'total_value' in engage_data and isinstance(engage_data['total_value'], dict) and 'breakdowns' in engage_data['total_value'] and isinstance(engage_data['total_value']['breakdowns'], list) and len(engage_data['total_value']['breakdowns']) > 0 and isinstance(engage_data['total_value']['breakdowns'][0], dict) and 'results' in engage_data['total_value']['breakdowns'][0]:
             results_list = engage_data['total_value']['breakdowns'][0].get('results', [])
             if results_list: # Verifica se a lista de resultados não está vazia
                 for item in results_list:
                     if 'dimension_values' in item and item['dimension_values']:
                         engage_results.append({
                             'PÁGINA': params_var['ig_username'],
                             'FAIXA DE IDADE': item['dimension_values'][0],
                             'ENGAJAMENTO': item.get('value', 0)
                         })
        df_engage_age = pd.DataFrame(engage_results) if engage_results else df_engage_age_default

        follower_data_metric = next((item for item in basic_insight['data'] if item['name'] == 'follower_demographics'), None)
        follower_results = []
        if follower_data_metric and 'total_value' in follower_data_metric and isinstance(follower_data_metric['total_value'], dict) and 'breakdowns' in follower_data_metric['total_value'] and isinstance(follower_data_metric['total_value']['breakdowns'], list) and len(follower_data_metric['total_value']['breakdowns']) > 0 and isinstance(follower_data_metric['total_value']['breakdowns'][0], dict) and 'results' in follower_data_metric['total_value']['breakdowns'][0]:
             results_list = follower_data_metric['total_value']['breakdowns'][0].get('results', [])
             if results_list:
                 for item in results_list:
                      follower_results.append({
                         'SEGUIDORES': item.get('value', 0),
                         'DATA DE LEITURA': now
                     })
        df_follower_age = pd.DataFrame(follower_results) if follower_results else df_follower_age_default

        max_len = max(len(df_engage_age), len(df_follower_age))
        df_engage_age = df_engage_age.reindex(range(max_len)).fillna({'PÁGINA': params_var['ig_username']})
        df_follower_age = df_follower_age.reindex(range(max_len)).fillna({'DATA DE LEITURA': now})

        df_age = pd.concat([df_engage_age, df_follower_age], axis=1)
        df_age = df_age.loc[:, ~df_age.columns.duplicated()]

    except (KeyError, IndexError, TypeError) as e:
        logger.exception(f"Erro ao processar dados de idade para {params_var['ig_username']}: {e}. Resposta API: {str(basic_insight)[:500]}")
        return df_age_default

    logger.info(f"Dados de idade extraídos com sucesso para {params_var['ig_username']}.")
    return df_age


def extrai_demografia(params_var: dict) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Extrai dados demográficos de cidade (seguidores e engajamento)."""
    logger.info(f"Extraindo dados de demografia (cidade) para {params_var['ig_username']}...")
    metrics = 'engaged_audience_demographics,follower_demographics'
    basic_insight = extrai_metricas_insight(params_var, metrics, breakdown='city')

    df_engage_city_default = pd.DataFrame(columns=['PÁGINA', 'CIDADES', 'ENGAJAMENTO', 'DATA DE LEITURA'])
    df_follower_city_default = pd.DataFrame(columns=['PÁGINA', 'CIDADES', 'SEGUIDORES', 'DATA DE LEITURA'])

    if not basic_insight:
        logger.warning(f"Não foi possível obter insights de demografia (cidade) para {params_var['ig_username']}. Retornando DataFrames padrão.")
        return df_follower_city_default, df_engage_city_default

    try:
        engage_data = next((item for item in basic_insight['data'] if item['name'] == 'engaged_audience_demographics'), None)
        engage_results = []
        if engage_data and 'total_value' in engage_data and isinstance(engage_data['total_value'], dict) and 'breakdowns' in engage_data['total_value'] and isinstance(engage_data['total_value']['breakdowns'], list) and len(engage_data['total_value']['breakdowns']) > 0 and isinstance(engage_data['total_value']['breakdowns'][0], dict) and 'results' in engage_data['total_value']['breakdowns'][0]:
             results_list = engage_data['total_value']['breakdowns'][0].get('results', [])
             if results_list:
                 for item in results_list:
                     if 'dimension_values' in item and item['dimension_values']:
                         engage_results.append({
                             'PÁGINA': params_var['ig_username'],
                             'CIDADES': item['dimension_values'][0],
                             'ENGAJAMENTO': item.get('value', 0),
                             'DATA DE LEITURA': now
                         })
        df_engage_city = pd.DataFrame(engage_results).sort_values(by=['ENGAJAMENTO'], ascending=False) if engage_results else df_engage_city_default

        follower_data_metric = next((item for item in basic_insight['data'] if item['name'] == 'follower_demographics'), None)
        follower_results = []
        if follower_data_metric and 'total_value' in follower_data_metric and isinstance(follower_data_metric['total_value'], dict) and 'breakdowns' in follower_data_metric['total_value'] and isinstance(follower_data_metric['total_value']['breakdowns'], list) and len(follower_data_metric['total_value']['breakdowns']) > 0 and isinstance(follower_data_metric['total_value']['breakdowns'][0], dict) and 'results' in follower_data_metric['total_value']['breakdowns'][0]:
             results_list = follower_data_metric['total_value']['breakdowns'][0].get('results', [])
             if results_list:
                 for item in results_list:
                     if 'dimension_values' in item and item['dimension_values']:
                          follower_results.append({
                             'PÁGINA': params_var['ig_username'],
                             'CIDADES': item['dimension_values'][0],
                             'SEGUIDORES': item.get('value', 0),
                             'DATA DE LEITURA': now
                         })
        df_follower_city = pd.DataFrame(follower_results).sort_values(by=['SEGUIDORES'], ascending=False) if follower_results else df_follower_city_default

    except (KeyError, IndexError, TypeError) as e:
        logger.exception(f"Erro ao processar dados de demografia para {params_var['ig_username']}: {e}. Resposta API: {str(basic_insight)[:500]}")
        return df_follower_city_default, df_engage_city_default

    logger.info(f"Dados de demografia (cidade) extraídos com sucesso para {params_var['ig_username']}.")
    return df_follower_city, df_engage_city


def extrai_entrega(params_var: dict, df_int: pd.DataFrame) -> pd.DataFrame:
    """Extrai métricas de entrega do dia anterior."""
    logger.info(f"Extraindo dados de entrega (dia anterior) para {params_var['ig_username']}...")

    data_ontem = datetime.now() - timedelta(days=1)
    start_utc = datetime(data_ontem.year, data_ontem.month, data_ontem.day, 0, 0, 0, tzinfo=timezone.utc)
    end_utc = datetime(data_ontem.year, data_ontem.month, data_ontem.day, 23, 59, 59, tzinfo=timezone.utc)
    since_unix = int(start_utc.timestamp())
    until_unix = int(end_utc.timestamp())

    url = params_var['endpoint_base'] + params_var['instagram_account_id'] + '/insights'
    page_id = params_var.get('page_id') # Para o rate limiter
    endpointParams = {
        'metric': 'total_interactions,likes,comments,saves,shares,replies',
        'access_token': params_var['access_token'],
        'period': 'day',
        'metric_type': 'total_value', # Deve ser total_value para essas métricas diárias
        'since': since_unix,
        'until': until_unix
    }

    basic_insight = make_request_with_retry(url, params=endpointParams, page_id=page_id)

    df_general_default = df_int.copy()
    for col in ['INTERAÇÕES', 'LIKES', 'COMENTÁRIOS', 'SALVAMENTOS', 'COMPARTILHAMENTOS', 'RESPOSTAS', 'DATA DE LEITURA']:
        if col not in df_general_default.columns:
             df_general_default[col] = 0 if col != 'DATA DE LEITURA' else now
    df_general_default['DATA DE LEITURA'] = now


    if not basic_insight or 'data' not in basic_insight: # Erro já logado por extrai_metricas_insight se basic_insight for None
        if basic_insight and 'data' not in basic_insight: # Log específico se a requisição foi OK mas faltou 'data'
            logger.error(f"Resposta da API para dados de entrega de {params_var['ig_username']} não contém a chave 'data'. Resposta: {str(basic_insight)[:500]}")
        # Se basic_insight for None, extrai_metricas_insight já logou.
        logger.error(f"Falha ao buscar dados de entrega para {params_var['ig_username']}. Retornando DataFrame geral padrão.")
        return df_general_default

    try:
        df_engage_api = pd.DataFrame(basic_insight['data'])

        def get_metric_value(df, name):
            row = df[df['name'] == name]
            # A API para métricas 'day' costuma retornar 'values' com um único dicionário
            if not row.empty and 'values' in row.iloc[0] and isinstance(row.iloc[0]['values'], list) and row.iloc[0]['values']:
                return row.iloc[0]['values'][0].get('value', 0)
            # Fallback para 'total_value' se a estrutura mudar
            elif not row.empty and 'total_value' in row.iloc[0] and isinstance(row.iloc[0]['total_value'], dict):
                 return row.iloc[0]['total_value'].get('value', 0)
            logger.warning(f"Métrica '{name}' não encontrada ou em formato inesperado na resposta de entrega.")
            return 0

        engage_dict = {
            'INTERAÇÕES': [get_metric_value(df_engage_api, 'total_interactions')],
            'LIKES': [get_metric_value(df_engage_api, 'likes')],
            'COMENTÁRIOS': [get_metric_value(df_engage_api, 'comments')],
            'SALVAMENTOS': [get_metric_value(df_engage_api, 'saves')],
            'COMPARTILHAMENTOS': [get_metric_value(df_engage_api, 'shares')],
            'RESPOSTAS': [get_metric_value(df_engage_api, 'replies')],
            'DATA DE LEITURA': [now]
        }

        df_engage = pd.DataFrame(engage_dict)
        df_general = pd.concat([df_int.reset_index(drop=True), df_engage.reset_index(drop=True)], axis=1)

    except (KeyError, IndexError, TypeError) as e:
        logger.exception(f"Erro ao processar dados de entrega para {params_var['ig_username']}: {e}. Resposta API: {str(basic_insight)[:500]}")
        return df_general_default

    logger.info(f"Dados de entrega extraídos com sucesso para {params_var['ig_username']}.")
    return df_general


def extrai_genero(params_var: dict) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Extrai dados demográficos de gênero."""
    logger.info(f"Extraindo dados de gênero para {params_var['ig_username']}...")
    metrics = 'engaged_audience_demographics,follower_demographics'
    basic_insight = extrai_metricas_insight(params_var, metrics, breakdown='gender')

    df_gender_base_cols = ['PÁGINA', 'DATA LEITURA', 'TIPO', 'FEMININO', 'MASCULINO', 'INDEFINIDO']
    df_engage_gender_default = pd.DataFrame(columns=df_gender_base_cols)
    df_follow_gender_default = pd.DataFrame(columns=df_gender_base_cols)
    df_gender_default = pd.DataFrame(columns=df_gender_base_cols)

    if not basic_insight:
        logger.warning(f"Não foi possível obter insights de gênero para {params_var['ig_username']}. Retornando DataFrames de gênero padrão.")
        return df_engage_gender_default, df_follow_gender_default, df_gender_default

    def process_gender_data(data_list, metric_name, tipo_label, username):
        """Processa os resultados de gênero para uma métrica específica."""
        metric_data = next((item for item in data_list if item['name'] == metric_name), None)
        gender_values = {'FEMININO': 0, 'MASCULINO': 0, 'INDEFINIDO': 0}

        if metric_data and 'total_value' in metric_data and isinstance(metric_data['total_value'], dict) and 'breakdowns' in metric_data['total_value'] and isinstance(metric_data['total_value']['breakdowns'], list) and len(metric_data['total_value']['breakdowns']) > 0 and isinstance(metric_data['total_value']['breakdowns'][0], dict) and 'results' in metric_data['total_value']['breakdowns'][0]:
             results_list = metric_data['total_value']['breakdowns'][0].get('results', [])
             if results_list:
                 for item in results_list:
                     if 'dimension_values' in item and item['dimension_values']:
                         gender = item['dimension_values'][0].upper()
                         value = item.get('value', 0)
                         if 'F' in gender: gender_values['FEMININO'] = value
                         elif 'M' in gender: gender_values['MASCULINO'] = value
                         elif 'U' in gender: gender_values['INDEFINIDO'] = value
                         else: logger.warning(f"Dimensão de gênero inesperada: {gender}")

        return pd.DataFrame([{
            'PÁGINA': username,
            'DATA LEITURA': now,
            'TIPO': tipo_label,
            'FEMININO': gender_values['FEMININO'],
            'MASCULINO': gender_values['MASCULINO'],
            'INDEFINIDO': gender_values['INDEFINIDO']
        }])

    try:
        api_data = basic_insight.get('data', [])
        df_engage_gender = process_gender_data(api_data, 'engaged_audience_demographics', 'ENGAJAMENTO', params_var['ig_username'])
        df_follow_gender = process_gender_data(api_data, 'follower_demographics', 'SEGUIDORES', params_var['ig_username'])
        df_gender = pd.concat([df_engage_gender, df_follow_gender], ignore_index=True)

    except (KeyError, IndexError, TypeError) as e:
        logger.exception(f"Erro ao processar dados de gênero para {params_var['ig_username']}: {e}. Resposta API: {str(basic_insight)[:500]}")
        return df_engage_gender_default, df_follow_gender_default, df_gender_default

    logger.info(f"Dados de gênero extraídos com sucesso para {params_var['ig_username']}.")
    return df_engage_gender, df_follow_gender, df_gender


def extrai_interactions(params_var: dict) -> Optional[pd.DataFrame]:
    """Extrai interações gerais da conta (visualizações, alcance)."""
    logger.info(f"Extraindo interações gerais (views, reach) para {params_var['ig_username']}...")
    # Remove follower_count daqui, pois é obtido via business_discovery
    metrics = 'views,reach'
    basic_insight = extrai_metricas_insight(params_var, metrics) 

    if not basic_insight or 'data' not in basic_insight: # Erro já logado por extrai_metricas_insight se basic_insight for None
        if basic_insight and 'data' not in basic_insight:
            logger.error(f"Resposta da API para interações gerais de {params_var['ig_username']} não contém a chave 'data'. Resposta: {str(basic_insight)[:500]}")
        logger.error(f"Falha ao buscar interações gerais (views, reach) para {params_var['ig_username']}. Retornando None.")
        return None

    try:
        df_interactions = pd.DataFrame(basic_insight['data'])
        # Verifica se as métricas esperadas estão presentes
        required_metrics = {'views', 'reach'}
        present_metrics = set(df_interactions['name'])
        if not required_metrics.issubset(present_metrics):
             logger.warning(f"Algumas métricas de interação esperadas ({required_metrics - present_metrics}) não foram encontradas para {params_var['ig_username']}. Resposta: {str(basic_insight)[:500]}")

        logger.info(f"Interações gerais (views, reach) extraídas com sucesso para {params_var['ig_username']}.")
        return df_interactions

    except (KeyError, IndexError, TypeError) as e:
        logger.exception(f"Erro ao processar dados de interações gerais (views, reach) para {params_var['ig_username']}: {e}. Resposta API: {str(basic_insight)[:500]}")
        return None


def extrai_followers(params_var: dict) -> int:
    """Extrai a contagem de seguidores via business_discovery."""
    logger.info(f"Extraindo contagem de seguidores (business_discovery) para {params_var['ig_username']}...")
    username_only = params_var['ig_username'][1:] if params_var['ig_username'].startswith('@') else params_var['ig_username']
    page_id = params_var.get('page_id') # Para o rate limiter

    url = params_var['endpoint_base'] + params_var['instagram_account_id']
    endpointParams = {
        'fields': f'business_discovery.username({username_only}){{followers_count}}',
        'access_token': params_var['access_token']
    }

    basic_insight = make_request_with_retry(url, params=endpointParams, page_id=page_id)

    if not basic_insight: # make_request_with_retry já logou o erro
        logger.error(f"Falha ao buscar contagem de seguidores para {params_var['ig_username']} (business_discovery). Retornando 0.")
        return 0

    try:
        # Verifica se 'business_discovery' e 'followers_count' existem antes de acessá-los
        if 'business_discovery' in basic_insight and isinstance(basic_insight['business_discovery'], dict) and \
           'followers_count' in basic_insight['business_discovery']:
            followers_count = basic_insight['business_discovery']['followers_count']
            logger.info(f"Contagem de seguidores extraída com sucesso para {params_var['ig_username']}: {followers_count}")
            return int(followers_count)
        else:
            logger.error(f"Resposta inesperada da API ao buscar contagem de seguidores para {params_var['ig_username']}. Chaves 'business_discovery' ou 'followers_count' ausentes. Resposta: {str(basic_insight)[:500]}")
            return 0
    except (KeyError, IndexError, TypeError, ValueError) as e:
        logger.exception(f"Erro ao processar contagem de seguidores para {params_var['ig_username']}: {e}. Resposta API: {str(basic_insight)[:500]}")
        return 0


def extrai_insights_midia(media_ids: List[str], params_var: dict, metrics: str) -> pd.DataFrame:
    """
    Extrai insights para uma lista de IDs de mídia (Reels ou Imagens/Carrossel).
    Usa a função auxiliar de retry.
    """
    logger.info(f"Extraindo insights de mídia ({metrics}) para {len(media_ids)} itens ({params_var['ig_username']})...")
    all_insights_list = []
    page_id = params_var.get('page_id') # Para o rate limiter

    total_items = len(media_ids)
    for idx, media_id in enumerate(media_ids, start=1):
        url = params_var['endpoint_base'] + str(media_id) + '/insights'
        endpointParamsInsight = {
            'metric': metrics,
            'access_token': params_var['access_token']
        }

        json_data_temp = make_request_with_retry(url, params=endpointParamsInsight, page_id=page_id)

        if json_data_temp and 'data' in json_data_temp and json_data_temp['data']:
            temp_insights = {'IDS': str(media_id)}
            valid_insight = False
            for insight in json_data_temp['data']:
                 metric_name = insight.get('name')
                 value = 0
                 if 'values' in insight and isinstance(insight['values'], list) and insight['values']:
                     value = insight['values'][0].get('value', 0)
                 elif 'total_value' in insight and isinstance(insight['total_value'], dict):
                      value = insight['total_value'].get('value', 0)

                 # Mapeia nomes da API (NÃO cria mais colunas _insight)
                 if metric_name == 'reach': temp_insights['ALCANCE'] = value
                 elif metric_name == 'saved': temp_insights['SALVOS'] = value
                 elif metric_name == 'shares': temp_insights['COMPARTILHAMENTOS'] = value
                 elif metric_name == 'views': temp_insights['VIEWS'] = value
                 elif metric_name == 'total_interactions': temp_insights['ENGAJAMENTO'] = value # Para imagens
                 # Ignora comments e likes dos insights, usaremos os da mídia base
                 elif metric_name in ['comments', 'likes']: pass
                 else: logger.debug(f"Métrica não mapeada encontrada: {metric_name} para ID {media_id}")

                 valid_insight = True

            if valid_insight:
                 all_insights_list.append(temp_insights)
            else:
                 logger.warning(f"Nenhuma métrica válida processada para ID {media_id}, embora a requisição tenha sido bem-sucedida. Resposta: {str(json_data_temp)[:300]}")

        elif json_data_temp and not json_data_temp.get('data'): # Requisição OK, mas 'data' está vazia ou ausente
             logger.warning(f"Requisição para insights da mídia ID {media_id} bem-sucedida, mas 'data' está vazia ou ausente. Resposta: {str(json_data_temp)[:300]}.")
        # else: make_request_with_retry já logou o erro de requisição

        # Log de progresso a cada 10 itens e no final
        if idx % 10 == 0 or idx == total_items:
            pct = (idx / total_items) * 100 if total_items else 100
            logger.info(f"Progresso insights ({metrics}) para {params_var['ig_username']}: {idx}/{total_items} ({pct:.1f}%).")
        else:
            logger.debug(f"Progresso insights ({metrics}): {idx}/{total_items}")

    if not all_insights_list:
        logger.warning(f"Nenhum insight de mídia coletado para a lista fornecida ({params_var['ig_username']}).")
        cols = ['IDS']
        if 'reach' in metrics: cols.append('ALCANCE')
        if 'saved' in metrics: cols.append('SALVOS')
        if 'shares' in metrics: cols.append('COMPARTILHAMENTOS')
        if 'views' in metrics: cols.append('VIEWS')
        if 'total_interactions' in metrics: cols.append('ENGAJAMENTO')
        return pd.DataFrame(columns=cols)

    df_insights = pd.DataFrame(all_insights_list)
    logger.info(f"Insights de mídia coletados para {len(df_insights)} itens ({params_var['ig_username']}).")
    return df_insights


def extrai_comentarios(media_ids: List[str], params_var: dict, page_username: str, stopwords_set: set) -> pd.DataFrame:
    """Extrai APENAS a primeira página de comentários para uma lista de IDs de mídia."""
    logger.info(f"Extraindo comentários (PRIMEIRA PÁGINA APENAS) para {len(media_ids)} posts ({page_username})...")
    all_comments_list = []
    max_comment_pages = 1 # Alterado para 1 para pegar apenas a primeira página
    page_id = params_var.get('page_id') # Para o rate limiter

    total_media = len(media_ids)
    for idx, media_id in enumerate(media_ids, start=1):
        logger.debug(f"Buscando comentários para post ID: {media_id}")
        comments_data = []
        page_count = 0
        next_url = params_var['endpoint_base'] + str(media_id) + '/comments'
        request_params = {'access_token': params_var['access_token']}

        # O loop executa no máximo uma vez devido a max_comment_pages = 1
        while next_url and page_count < max_comment_pages:
            page_count += 1
            logger.debug(f"Buscando página {page_count} de comentários para post {media_id}...")
            json_data_temp = make_request_with_retry(next_url, params=request_params, page_id=page_id) # request_params só é usado na primeira chamada
            request_params = None # Params só necessários na primeira chamada (next_url já os contém)

            if json_data_temp and 'data' in json_data_temp:
                current_page_comments = json_data_temp['data']
                if current_page_comments:
                     logger.debug(f"Recebidos {len(current_page_comments)} comentários na página {page_count} para post {media_id}.")
                     comments_data.extend(current_page_comments)

                next_url = None

            elif json_data_temp:
                logger.warning(f"Chave 'data' não encontrada na resposta da página {page_count} de comentários para post ID {media_id}. Resposta: {str(json_data_temp)[:300]}")
                next_url = None
            else:
                 logger.warning(f"Falha ao buscar página {page_count} de comentários para post {media_id}. A requisição pode ter falhado ou a resposta não continha 'data'.") # Mensagem mais genérica
                 next_url = None

        if comments_data:
            temp_df = pd.DataFrame(comments_data)
            temp_df['post_id'] = f'{media_id}A'
            temp_df['pagina'] = page_username
            all_comments_list.append(temp_df)

        # Log de progresso a cada 10 posts e no final
        if idx % 10 == 0 or idx == total_media:
            pct = (idx / total_media) * 100 if total_media else 100
            logger.info(f"Progresso comentários ({page_username}): {idx}/{total_media} ({pct:.1f}%).")
        else:
            logger.debug(f"Progresso comentários: {idx}/{total_media}")

    if not all_comments_list:
        logger.warning(f"Nenhum comentário encontrado (na primeira página) para os posts fornecidos ({page_username}).")
        return pd.DataFrame(columns=['id', 'text', 'timestamp', 'username', 'post_id', 'pagina'])

    df_comments = pd.concat(all_comments_list, ignore_index=True)

    if 'text' in df_comments.columns:
        logger.info(f"Processando texto de {len(df_comments)} comentários...")
        df_comments['text'] = df_comments['text'].astype(str).apply(remover_acentos)
        df_comments['text'] = df_comments['text'].apply(lambda x: remover_stopwords(x, stopwords_set)).str.lower()
    else: 
        logger.warning("Coluna 'text' não encontrada no DataFrame de comentários.")

    logger.info(f"Total de {len(df_comments)} comentários extraídos (primeira página) e processados para {page_username}.")
    return df_comments


# Função antiga de salvamento em CSV/Excel removida


def get_data(params_var: dict, endpointParams_var: dict, hashtag_var: list, stopwords_set: set):
    """Função principal para obter e processar dados de uma conta."""
    page_username = params_var['ig_username']
    logger.info(f"### Processando conta: {page_username} ###")

    # 1. Buscar Mídias Básicas
    logger.info(f"Buscando mídias básicas para {page_username}...")
    df_variable, _ = fetch_media_data(params_var, endpointParams_var) # fetch_media_data agora faz sys.exit se a primeira página falhar

    if df_variable is None or df_variable.empty: # Se fetch_media_data não fez sys.exit mas retornou None/vazio (ex: paginação falhou, mas não a primeira)
        logger.error(f"Não foi possível obter mídias básicas suficientes para {page_username} após tentativas. Abortando processamento desta conta.")
        # Define colunas vazias para todos os DataFrames que seriam retornados
        empty_df_cols = ['IDS', 'TIPO', 'TITULO', 'DATA', 'LINK', 'PUBLICAÇÃO', 'LIKES', 'COMENTÁRIOS', 'IDADE DA POSTAGEM', 'DATA DE LEITURA', 'HORÁRIO DA POSTAGEM', 'HASHTAG', 'VIEWS', 'ALCANCE', 'SALVOS', 'COMPARTILHAMENTOS', 'ENGAJAMENTO', 'PAGINA', 'THUMBNAIL']
        empty_general_cols = ['PÁGINA', 'SEGUIDORES', 'VIEWS', 'ALCANCE', 'VISUALIZAÇÕES AO PERFIL', 'CLIQUES NO LINK', 'INTERAÇÕES', 'LIKES', 'COMENTÁRIOS', 'SALVAMENTOS', 'COMPARTILHAMENTOS', 'RESPOSTAS', 'DATA DE LEITURA']
        empty_gender_cols = ['PÁGINA', 'DATA LEITURA', 'TIPO', 'FEMININO', 'MASCULINO', 'INDEFINIDO']
        empty_age_cols = ['PÁGINA', 'FAIXA DE IDADE', 'ENGAJAMENTO', 'SEGUIDORES', 'DATA DE LEITURA']
        empty_city_cols_base = ['PÁGINA', 'CIDADES', 'DATA DE LEITURA'] # Base para engajamento e seguidores
        empty_engage_city_cols = empty_city_cols_base + ['ENGAJAMENTO']
        empty_follower_city_cols = empty_city_cols_base + ['SEGUIDORES']
        empty_tag_cols = ['IDS', 'HASHTAG', 'DATA DE LEITURA', 'DATA']
        empty_comment_cols = ['id', 'text', 'timestamp', 'username', 'post_id', 'pagina']
        
        # Retorna DataFrames vazios com as colunas esperadas
        return (pd.DataFrame(columns=empty_df_cols), 
                pd.DataFrame(columns=empty_general_cols), 
                pd.DataFrame(columns=empty_gender_cols), 
                pd.DataFrame(columns=empty_age_cols), 
                pd.DataFrame(columns=empty_engage_city_cols), # Para df_engage_city
                pd.DataFrame(columns=empty_follower_city_cols), # Para df_follower_city
                pd.DataFrame(columns=empty_tag_cols), 
                pd.DataFrame(columns=empty_comment_cols), # Para one_day_variable
                pd.DataFrame(columns=empty_comment_cols)) # Para seven_days_variable


    # 2. Processamento Inicial do DataFrame
    logger.info(f"Organizando tabela de mídias básicas para {page_username}...")
    try: 
        rename_map = {}
        base_cols = {'id':'IDS', 'media_type':'TIPO', 'timestamp':'DATA', 'permalink':'LINK', 'media_product_type':'PUBLICAÇÃO', 'like_count':'LIKES', 'comments_count':'COMENTÁRIOS'}
        if 'caption' in df_variable.columns: base_cols['caption'] = 'TITULO'
        elif 'caption_text' in df_variable.columns: base_cols['caption_text'] = 'TITULO'
        for api_col, new_col in base_cols.items():
             if api_col in df_variable.columns: rename_map[api_col] = new_col
        df_variable = df_variable.rename(columns=rename_map)
        for col in ['TITULO', 'LIKES', 'COMENTÁRIOS']:
             if col not in df_variable.columns: df_variable[col] = '' if col == 'TITULO' else 0


        #  ▸ 2A. Renomeia os novos campos brutos
        #    (Atualiza o rename_map existente em vez de criar um novo)
        for api_col, new_col in [('media_url', 'MEDIA_URL'), ('thumbnail_url', 'THUMB_URL')]:
            if api_col in df_variable.columns:
                # Adiciona ao rename_map existente se a coluna existir
                rename_map[api_col] = new_col
        df_variable = df_variable.rename(columns=rename_map) # Renomeia novamente com os novos campos

        #  ▸ 2B. Cria a coluna final THUMBNAIL
        if 'THUMB_URL' not in df_variable.columns:
            df_variable['THUMB_URL'] = ''
        if 'MEDIA_URL' not in df_variable.columns:
            df_variable['MEDIA_URL'] = ''

        # Certifica que TIPO existe antes de usar na lambda
        if 'TIPO' not in df_variable.columns:
            df_variable['TIPO'] = '' # Ou algum valor padrão apropriado

        df_variable['THUMBNAIL'] = df_variable.apply(
            lambda r: r['THUMB_URL'] if isinstance(r['TIPO'], str) and r['TIPO'].upper() == 'VIDEO' else r['MEDIA_URL'],
            axis=1
        )

        df_variable.drop(columns=['THUMB_URL', 'MEDIA_URL'], errors='ignore', inplace=True)


        # Processamento de Datas Vetorizado
        logger.debug(f"Coluna DATA antes da conversão (primeiros 5): {df_variable['DATA'].head().tolist()}")
        # 1) Converte toda a coluna para datetime já c/ timezone UTC
        dt_utc = pd.to_datetime(df_variable['DATA'], utc=True, errors='coerce')
        # Trata NaT (datas inválidas) antes de converter timezone
        valid_dates_mask = dt_utc.notna()
        dt_local = pd.Series(pd.NaT, index=df_variable.index, dtype='datetime64[ns, Etc/GMT+3]') # Cria Series de NaT com tipo correto

        if valid_dates_mask.any():
            # 2) Converte o timezone para GMT-3 (Brasília) - Somente para datas válidas
            dt_local.loc[valid_dates_mask] = dt_utc[valid_dates_mask].dt.tz_convert('Etc/GMT+3')
            logger.debug(f"Primeiras 5 datas convertidas para GMT-3: {dt_local[valid_dates_mask].head().tolist()}")
        else:
            logger.warning(f"Nenhuma data válida encontrada na coluna 'DATA' para {page_username} após conversão inicial.")

        #  Processamento de Datas Vetorizado (tz-naive) 
        # 1) normalize() (zera hora) -> 2) remove timezone -> 3) armazena numa coluna tz-naive
        df_variable['DATA_dt'] = dt_local.dt.normalize().dt.tz_localize(None) # Agora tz-naive

        # Hora da postagem (ainda usa dt_local tz-aware para pegar a hora correta)
        df_variable['HORÁRIO DA POSTAGEM'] = dt_local.dt.strftime('%H:%M:%S')
        # Preenche NaT com string vazia para consistência, se houver NaT em dt_local
        df_variable['HORÁRIO DA POSTAGEM'] = df_variable['HORÁRIO DA POSTAGEM'].fillna('')

        # Idade da postagem: crie um Timestamp do 'hoje' (tz-naive) e subtraia
        now_ts = pd.Timestamp(now)  # Cria Timestamp tz-naive de 'now' (que é date)
        df_variable['IDADE DA POSTAGEM'] = (now_ts - df_variable['DATA_dt']).dt.days
        # Preenche NaT/erros no cálculo da idade com -1
        df_variable['IDADE DA POSTAGEM'] = df_variable['IDADE DA POSTAGEM'].fillna(-1).astype(int)

        # Define a data de leitura
        df_variable['DATA DE LEITURA'] = now

        # Coluna DATA formatada a partir de DATA_dt (que agora é tz-naive)
        df_variable['DATA'] = df_variable['DATA_dt'].dt.strftime('%Y-%m-%d')
        df_variable['DATA'] = df_variable['DATA'].replace('NaT', pd.NA) # Substitui 'NaT' string por NA real

        df_variable['TITULO'] = df_variable['TITULO'].fillna('').astype(str).str.lower()
        # A formatação da coluna DATA foi movida para cima

        # As conversões de LIKES, COMENTÁRIOS e IDADE DA POSTAGEM podem continuar aqui ou ser ajustadas
        # IDADE DA POSTAGEM já foi tratada acima
        df_variable['LIKES'] = pd.to_numeric(df_variable['LIKES'], errors='coerce').fillna(0).astype(int)
        df_variable['COMENTÁRIOS'] = pd.to_numeric(df_variable['COMENTÁRIOS'], errors='coerce').fillna(0).astype(int)

        df_range_variable = df_variable[(df_variable['IDADE DA POSTAGEM'] >= 0) & (df_variable['IDADE DA POSTAGEM'] <= 7)].copy()
        if df_range_variable.empty: logger.warning(f"Nenhum post encontrado nos últimos 7 dias para {page_username}.")

    except Exception as e: 
        logger.exception(f"Erro crítico durante processamento inicial do DataFrame para {page_username}: {e}. Abortando esta conta. Saindo...")
        # Retorna DataFrames vazios
        sys.exit(1)

    # 3. Extrair Hashtags 
    df_tag = pd.DataFrame(columns=['IDS', 'HASHTAG', 'DATA DE LEITURA', 'DATA'])
    if not df_range_variable.empty:
        logger.info(f"Extraindo hashtags para {page_username}...")
        df_range_variable['HASHTAG'] = df_range_variable['TITULO'].apply(lambda x: encontrar_hashtags(x, hashtag_var))
        dfs_to_concat_tags = []
        for _, row in df_range_variable.iterrows():
            hashtags = row['HASHTAG']
            if hashtags:
                id_val, data_val = row['IDS'], row['DATA']
                for hashtag in hashtags:
                    dfs_to_concat_tags.append(pd.DataFrame({'IDS': [f'{id_val}A'], 'HASHTAG': [hashtag], 'DATA DE LEITURA': [now], 'DATA': [data_val]}))
        if dfs_to_concat_tags: df_tag = pd.concat(dfs_to_concat_tags, ignore_index=True)
        logger.info(f"Extraídas {len(df_tag)} ocorrências de hashtags monitoradas para {page_username}.")
    else: logger.info(f"Pulando extração de hashtags (nenhum post recente) para {page_username}.")

    # 4. Extrair Comentários
    logger.info(f"Extraindo comentários (1 dia) para {page_username}...")
    one_day_ids = df_variable[df_variable['IDADE DA POSTAGEM'] == 1]['IDS'].tolist()
    one_day_variable = extrai_comentarios(one_day_ids, params_var, page_username, processed_stopwords)
    logger.info(f"Extraindo comentários (7 dias) para {page_username}...")
    seven_day_ids = df_range_variable['IDS'].tolist() if not df_range_variable.empty else []
    seven_days_variable = extrai_comentarios(seven_day_ids, params_var, page_username, processed_stopwords)

    # 5. Separar Mídias e Extrair Insights
    df_reels_insights, df_img_insights = pd.DataFrame(), pd.DataFrame()
    if not df_range_variable.empty:
        df_reels_range = df_range_variable[df_range_variable['PUBLICAÇÃO'].str.contains('REELS', na=False)].copy()
        df_images_range = df_range_variable[df_range_variable['TIPO'].isin(['IMAGE', 'CAROUSEL_ALBUM'])].copy()
        if not df_reels_range.empty:
            logger.info(f"Extraindo insights para {len(df_reels_range)} Reels ({page_username})...")
            df_reels_insights = extrai_insights_midia(df_reels_range['IDS'].tolist(), params_var, metrics='reach,saved,shares,views') # Removido comments, likes
        else: logger.info(f"Nenhum Reel encontrado nos últimos 7 dias para {page_username}.")
        if not df_images_range.empty:
            logger.info(f"Extraindo insights para {len(df_images_range)} Imagens/Carrosséis ({page_username})...")
            df_img_insights = extrai_insights_midia(df_images_range['IDS'].tolist(), params_var, metrics='total_interactions,reach,saved,views')
        else: logger.info(f"Nenhuma Imagem/Carrossel encontrado nos últimos 7 dias para {page_username}.")
    else: logger.info(f"Pulando extração de insights de mídia (nenhum post recente) para {page_username}.")

    # 6. Extrair Insights Gerais da Conta
    logger.info(f"Extraindo insights gerais da conta para {page_username}...")
    followers_count = extrai_followers(params_var)
    df_interactions = extrai_interactions(params_var)
    ind_dict = {'PÁGINA': [page_username], 'SEGUIDORES': [followers_count], 'VIEWS': 0, 'ALCANCE': 0, 'VISUALIZAÇÕES AO PERFIL': 0, 'CLIQUES NO LINK': 0}
    if df_interactions is not None:
        try:
            # Atualiza get_interaction_value para tratar 'total_value' também
            def get_interaction_value(df, name):
                row = df[df['name'] == name]
                if row.empty:
                    # Métrica não veio: retorna 0 sem warning
                    return 0
                data = row.iloc[0]

                # 1) API devolveu lista de valores diários
                if 'values' in data and isinstance(data['values'], list) and data['values']:
                    vals = [v.get('value', 0) for v in data['values']]
                    # soma os últimos dois dias, se existirem (mantendo lógica original, embora 'day' period geralmente retorne 1)
                    return sum(vals[-2:])

                # 2) API devolveu total_value (porque passou metric_type='total_value')
                if 'total_value' in data:
                    tv = data['total_value']
                    if isinstance(tv, dict) and 'value' in tv:
                        return tv['value']
                    elif isinstance(tv, (int, float)):
                        # Caso total_value seja diretamente o valor numérico
                        return tv

                # 3) Nem values nem total_value: cai aqui
                logger.debug(f"Formato inesperado para métrica de interação '{name}' em {page_username}, retornando 0. Dados: {str(data)[:200]}")
                return 0

            ind_dict['VIEWS'] = [get_interaction_value(df_interactions, 'views')]
            ind_dict['ALCANCE'] = [get_interaction_value(df_interactions, 'reach')]
        except Exception as e: logger.exception(f"Erro ao processar df_interactions para {page_username}: {e}")
    df_int = pd.DataFrame(ind_dict)

    # 7. Extrair Entrega (Dia Anterior)
    df_general = extrai_entrega(params_var, df_int)

    # 8. Extrair Demográficos
    df_engage_gender, df_follow_gender, df_gender = extrai_genero(params_var)
    df_follower_city, df_engage_city = extrai_demografia(params_var)
    df_age = extrai_idade(params_var)

    # 9. Consolidar Insights de Mídia com Dados Básicos
    logger.info(f"Consolidando insights de mídia com dados básicos para {page_username}...")
    df_final_posts = pd.DataFrame()
    if not df_range_variable.empty:
        df_final_posts = df_range_variable.copy()
        # Merge REELS insights (se houver)
        if not df_reels_insights.empty:
            df_reels_insights['IDS'] = df_reels_insights['IDS'].astype(str)
            df_final_posts = pd.merge(df_final_posts, df_reels_insights, on='IDS', how='left')
        else: # Adiciona colunas de insight de Reels se não existirem
            for col in ['ALCANCE', 'SALVOS', 'COMPARTILHAMENTOS', 'VIEWS']:
                 if col not in df_final_posts.columns: df_final_posts[col] = 0
        # Merge IMAGE/CAROUSEL insights (se houver), usando sufixo e combinando
        if not df_img_insights.empty:
            df_img_insights['IDS'] = df_img_insights['IDS'].astype(str)
             # Usa um df temporário para merge e depois atualiza df_final_posts
            df_final_posts = pd.merge(df_final_posts, df_img_insights, on='IDS', how='left', suffixes=('', '_img'))
            for col in ['ENGAJAMENTO', 'ALCANCE', 'SALVOS', 'VIEWS']: # Colunas que podem vir de ambos
                col_img = f'{col}_img'
                if col_img in df_final_posts.columns:
                     # Combina: Usa valor _img se o original for NaN/0, senão mantém original
                     df_final_posts[col] = df_final_posts[col].fillna(0)
                     df_final_posts[col_img] = df_final_posts[col_img].fillna(0)
                     df_final_posts[col] = df_final_posts.apply(lambda row: row[col_img] if row[col] == 0 else row[col], axis=1)
                     df_final_posts = df_final_posts.drop(columns=[col_img])
        else: # Adiciona colunas de insight de Imagem se não existirem
             for col in ['ENGAJAMENTO', 'ALCANCE', 'SALVOS', 'VIEWS']:
                 if col not in df_final_posts.columns: df_final_posts[col] = 0
    else:
        # Quando não há posts nos últimos 7 dias, cria DataFrame vazio com estrutura padrão
        logger.info(f"Não há posts nos últimos 7 dias para {page_username}. Criando DataFrame com estrutura padrão.")
        df_final_posts = pd.DataFrame(columns=['IDS', 'TIPO', 'TITULO', 'DATA', 'LINK', 'PUBLICAÇÃO', 'LIKES', 'COMENTÁRIOS', 'IDADE DA POSTAGEM', 'DATA DE LEITURA', 'HORÁRIO DA POSTAGEM', 'ALCANCE', 'SALVOS', 'COMPARTILHAMENTOS', 'VIEWS', 'ENGAJAMENTO'])

    # 10. Adicionar Colunas Finais
    if not df_final_posts.empty:
        df_final_posts['PAGINA'] = page_username
    else:
        # Para DataFrame vazio, adicionar a coluna PAGINA
        df_final_posts['PAGINA'] = page_username

    # 11. Processamento Final e Cálculo de Engajamento
    logger.info(f"Realizando processamento final para {page_username}...")
    save_df = df_final_posts.copy()
    
    # Verifica se o DataFrame não está vazio e tem a coluna IDS
    if save_df.empty:
        logger.warning(f"DataFrame final está vazio para {page_username}. Criando DataFrame com estrutura padrão.")
        # Cria um DataFrame vazio com as colunas necessárias
        save_df = pd.DataFrame(columns=['IDS', 'TIPO', 'TITULO', 'DATA', 'LINK', 'PUBLICAÇÃO', 'LIKES', 'COMENTÁRIOS', 'IDADE DA POSTAGEM', 'DATA DE LEITURA', 'HORÁRIO DA POSTAGEM', 'HASHTAG', 'VIEWS', 'ALCANCE', 'SALVOS', 'COMPARTILHAMENTOS', 'ENGAJAMENTO', 'PAGINA', 'THUMBNAIL'])
    else:
        save_df = save_df.fillna(0)
        # Verifica se a coluna IDS existe antes de tentar acessá-la
        if 'IDS' not in save_df.columns:
            logger.error(f"ERRO CRÍTICO: Coluna 'IDS' não encontrada no DataFrame para {page_username}. Colunas disponíveis: {list(save_df.columns)}")
            logger.error(f"Encerrando execução devido a erro crítico de estrutura de dados.")
            sys.exit(1)
        
        save_df['IDS'] = save_df['IDS'].astype(str)
        ids_to_update = ~save_df['IDS'].str.endswith('A', na=False)
        save_df.loc[ids_to_update, 'IDS'] = save_df.loc[ids_to_update, 'IDS'] + 'A'

    # Verifica se há dados para processar engajamento
    if not save_df.empty:
        cols_for_engagement = ['LIKES', 'COMENTÁRIOS', 'SALVOS', 'COMPARTILHAMENTOS']
        for col in cols_for_engagement:
            if col not in save_df.columns: 
                save_df[col] = 0
            save_df[col] = pd.to_numeric(save_df[col], errors='coerce').fillna(0)
        save_df['ENGAJAMENTO_calc'] = save_df['LIKES'] + save_df['COMENTÁRIOS'] + save_df['SALVOS'] + save_df['COMPARTILHAMENTOS']
        if 'ENGAJAMENTO' in save_df.columns:
             save_df['ENGAJAMENTO'] = pd.to_numeric(save_df['ENGAJAMENTO'], errors='coerce').fillna(0)
             save_df['ENGAJAMENTO'] = save_df.apply(lambda row: row['ENGAJAMENTO_calc'] if row['ENGAJAMENTO'] == 0 else row['ENGAJAMENTO'], axis=1)
        else: 
            save_df['ENGAJAMENTO'] = save_df['ENGAJAMENTO_calc']
        save_df = save_df.drop(columns=['ENGAJAMENTO_calc'])
    else:
        # Para DataFrame vazio, garante que as colunas de engajamento existam
        cols_for_engagement = ['LIKES', 'COMENTÁRIOS', 'SALVOS', 'COMPARTILHAMENTOS', 'ENGAJAMENTO']
        for col in cols_for_engagement:
            if col not in save_df.columns:
                save_df[col] = 0

    cols_to_int = ['LIKES', 'COMENTÁRIOS', 'IDADE DA POSTAGEM', 'ALCANCE', 'SALVOS', 'COMPARTILHAMENTOS', 'VIEWS', 'ENGAJAMENTO']
    for col in cols_to_int:
        if col in save_df.columns: 
            if not save_df.empty:
                save_df[col] = pd.to_numeric(save_df[col], errors='coerce').fillna(0).astype(int)
            else:
                # Para DataFrame vazio, apenas garante que a coluna existe com valor padrão
                save_df[col] = 0
        else: 
            logger.info(f"Adicionando coluna final '{col}' para {page_username}.")
            save_df[col] = 0

    cols_general_to_int = ['SEGUIDORES', 'VIEWS', 'ALCANCE', 'VISUALIZAÇÕES AO PERFIL', 'CLIQUES NO LINK', 'INTERAÇÕES', 'LIKES', 'COMENTÁRIOS', 'SALVAMENTOS', 'COMPARTILHAMENTOS', 'RESPOSTAS']
    for col in cols_general_to_int:
        if col in df_general.columns: 
            if not df_general.empty:
                df_general[col] = pd.to_numeric(df_general[col], errors='coerce').fillna(0).astype(int)
            else:
                # Para DataFrame vazio, apenas garante que a coluna existe
                df_general[col] = 0
        else: 
            logger.info(f"Adicionando coluna final '{col}' em df_general para {page_username}.")
            df_general[col] = 0

    save_df_variable = save_df
    
    # Verificação final de integridade dos dados
    if save_df_variable is None:
        logger.error(f"ERRO CRÍTICO: save_df_variable é None para {page_username}")
        logger.error(f"Encerrando execução devido a erro crítico de processamento.")
        sys.exit(1)
    
    # Log de sucesso com informações sobre os dados processados
    if not save_df_variable.empty:
        logger.info(f"### Processamento concluído para: {page_username} - {len(save_df_variable)} posts processados ###")
    else:
        logger.info(f"### Processamento concluído para: {page_username} - Nenhum post recente encontrado ###")

    # Reindex para garantir colunas consistentes no retorno
    empty_df_cols = ['IDS', 'TIPO', 'TITULO', 'DATA', 'LINK', 'PUBLICAÇÃO', 'LIKES', 'COMENTÁRIOS', 'IDADE DA POSTAGEM', 'DATA DE LEITURA', 'HORÁRIO DA POSTAGEM', 'HASHTAG', 'VIEWS', 'ALCANCE', 'SALVOS', 'COMPARTILHAMENTOS', 'ENGAJAMENTO', 'PAGINA', 'THUMBNAIL']
    empty_general_cols = ['PÁGINA', 'SEGUIDORES', 'VIEWS', 'ALCANCE', 'VISUALIZAÇÕES AO PERFIL', 'CLIQUES NO LINK', 'INTERAÇÕES', 'LIKES', 'COMENTÁRIOS', 'SALVAMENTOS', 'COMPARTILHAMENTOS', 'RESPOSTAS', 'DATA DE LEITURA']
    empty_gender_cols = ['PÁGINA', 'DATA LEITURA', 'TIPO', 'FEMININO', 'MASCULINO', 'INDEFINIDO']
    empty_age_cols = ['PÁGINA', 'FAIXA DE IDADE', 'ENGAJAMENTO', 'SEGUIDORES', 'DATA DE LEITURA']
    # Colunas para df_engage_city e df_follower_city
    empty_city_cols_base = ['PÁGINA', 'CIDADES', 'DATA DE LEITURA']
    empty_engage_city_cols = empty_city_cols_base + ['ENGAJAMENTO']
    empty_follower_city_cols = empty_city_cols_base + ['SEGUIDORES']
    empty_tag_cols = ['IDS', 'HASHTAG', 'DATA DE LEITURA', 'DATA']
    empty_comment_cols = ['id', 'text', 'timestamp', 'username', 'post_id', 'pagina']

    # Função auxiliar para reindexar e preencher
    def reindex_and_fill(df, columns, fill_value=0):
        if df is None: # Adicionado para tratar df None
            logger.debug(f"DataFrame é None, retornando DataFrame vazio com colunas: {columns}")
            return pd.DataFrame(columns=columns)
        if not isinstance(df, pd.DataFrame): # Adicionado para tratar se não for DataFrame
            logger.warning(f"Esperado DataFrame, mas obtido {type(df)}. Retornando DataFrame vazio com colunas: {columns}")
            return pd.DataFrame(columns=columns)

        try:
            return df.reindex(columns=columns, fill_value=fill_value)
        except Exception as e:
            logger.error(f"Erro ao reindexar DataFrame para colunas {columns}: {e}. DataFrame original: {df.head() if not df.empty else 'vazio'}")
            return pd.DataFrame(columns=columns) # Retorna vazio em caso de erro


    save_df_variable = reindex_and_fill(save_df_variable, empty_df_cols)
    df_general = reindex_and_fill(df_general, empty_general_cols)
    df_gender = reindex_and_fill(df_gender, empty_gender_cols)
    df_age = reindex_and_fill(df_age, empty_age_cols)
    df_engage_city = reindex_and_fill(df_engage_city, empty_engage_city_cols)
    df_follower_city = reindex_and_fill(df_follower_city, empty_follower_city_cols)
    df_tag = reindex_and_fill(df_tag, empty_tag_cols)
    one_day_variable = reindex_and_fill(one_day_variable, empty_comment_cols, fill_value='') # Comentários podem ter strings
    seven_days_variable = reindex_and_fill(seven_days_variable, empty_comment_cols, fill_value='')


    return (save_df_variable, df_general, df_gender, df_age, df_engage_city, df_follower_city, df_tag, one_day_variable, seven_days_variable)

# --- Listas de Hashtags e Stopwords 
hashtag_list = ['eleições', 'política', 'opinião', 'economia', 'esporte', 'saúde', 'educação', 'meioambiente', 'enchentes', 'violência', 'show', 'cultura', 'celebridades', 'vídeosviralizados', 'acidente', 'trânsito', 'denúncia', 'colunagiro', 'charge', 'polícia']
hashtags_anhanguera = ['jornalismobastidoranhanguera', 'jornalismochanhanguera', 'jornalismoconteudoanhanguera', 'jornalismonacionalanhanguera', 'novela1anhanguera', 'novela2anhanguera', 'novela3anhanguera', 'edespecialanhanguera', 'valeapenaanhanguera', 'g1anhanguera', 'marketinganhanguera', 'memeanhanguera', 'nobalaioanhanguera', 'programasgloboanhanguera', 'geanhanguera', 'filmesanhanguera', 'empreenderanhanguera', 'regionalanhanguera', 'ativacaoanhanguera']
hashtags_moov = ['moovfm', 'segueobeat', 'rapnacional', 'breaking', 'grafite', 'batalhaderima']
hashtags_jornalismotv = ['jornalismotvanhanguera', 'tvanhangueratransito', 'tvanhanguerapolitica', 'JA1', 'JA2', 'BDG', 'JC']
hashtags_pratodia = ['trecho', 'promo', 'receita', 'local']
stopwords_list = ['parece', 'kkkk', 'kkkkk', 'quero', 'onde', 'médico', 'novo', 'doi', 'acho', 'casa', 'logo', 'outros', 'sempre', 'homem', 'mulher', 'pai', 'antes', 'tanto', 'saber', 'tem', 'vejo', 'nesse', 'c', 'próprio', 'sair', 'un', 'realmente', 'fala', 'um', 'mas', 'nas', 'desde', 'sobre', 'fazendo', 'pode', 'conta', 'uai', 'cadê', 'algo', 'anos', 'muita', 'carro', 'querem', 'se', 'feito', 'ficou', 'jeito', 'também', 'forma', 'frente', 'tipo', 'dessa', 'apena', 'vamo', 'vamos', 'deixa', 'chega', 'cada', 'toda', 'sai', 'dia', 'assim', 'parte', 'desses', 'tal', 'a', 'ano', 'quase', 'vez', 'sabe', 'alguem', 'alguém', 'fica', 'so', 'só', 'sei', 'ir', 'ma', 'deve', 'pro', 'nome', 'senhor', 'mais', 'colocar', 'mim', 'vi', 'tava', 'que', 'mulhere', 'vão', 'mão', 'olha', 'nessa', 'sendo', 'ficar', 'o', 'ja', 'quer', 'nao', 'meno', 'falar', 'isso', 'hora', 'dar', 'então', 'alguma', 'algum', 'dele', 'ele', 'viu', 'desse', 'vou', 'fez', 'vem', 'tão', 'ta', 'nada', 'fazer', 'faz', 'outra', 'outro', 'ver', 'tbm', 'é', 'ante', 'não', 'mesmo', 'sim', 'coisa','demai', 'pois', 'ter', 'vc', 'l', 'r', 'pra', 'q', 'lá', 'aí', 'ai', 'né', 'q ', 'la', 'poi', 'paí', 'tá', 'pessoa', 'cara', 'dá', 'da', 'e', 'tudo', 'todo', 'n', 'c ', 'e ', 'p', 'p ', 'l ', 'goiá', 'vai', 'e ', ' e', 'pessoa ', 'agora', 'deu', 'mai', 'ainda', 'né ', 'essa', 'sejam', 'pela', 'certo', 'aos', 'em', 'do', 'para', 'hummm', 'de', 'esse', 'vê', 'são', 'os', 'pelo', 'saem', 'quem', 'com', 'dos', 'vcs', 'eles', 'como', 'no', 'erário', 'termos', 'teve', 'dessas', 'todos', 'quando', 'visto', 'tornou', 'maximo', 'escro', 'ocorre', 'na', 'haja', 'sinonimo', 'aquela', 'uma', 'seu', 'já', 'as', 'por', 'ao', 'das', 'sem', 'ser', 'foi', 'por', 'ou', 'está', 'nem',' eu',' qual', 'muito', 'for', 'há', 'pq', 'minha', 'à',' vale', 'favor', 'tarde',' minha', 'kkk', 'ela', 'nao',' ate', 'ha', 'me', 'tao', 'ne',' nos', 'sua', 'bruno', 'alguns', 'passou', 'sua ', 'cao', 'qual', 'bem', 'tem',' esta','este','sera', 'estao',' esses', 'num',' dela', 'nos', 'aqui', 'sao', 'boa', 'eu', 'davi', 'era', 'meu', 'inclusive', 'seria', 'vao', 'seja', 'cade', 'fosse', 'estamos', 'b', 'existe', 'ate', 'porque', 'dizer', 'eram', 'dela', 'sido', 'voce', 'voltar', 'zero', 'entao', 'oba', 'm', 'tinha', 'atras', 'apos', 'esses', 'esta', 'pe', 'acs', 'te', 'd', 'essas', '.']

logger.info("Processando lista de stopwords...")
processed_stopwords = {remover_acentos(word) for word in stopwords_list}
logger.info(f"{len(processed_stopwords)} stopwords únicas processadas.")


# --- Leitura de Dados Antigos (via Banco) ---
def load_old_table(table_name: str) -> pd.DataFrame:
    logger.info(f"Lendo dados antigos da tabela: {table_name}")
    try:
        conn = get_db_connection()
        df = pd.read_sql(f"SELECT * FROM [dbo].[{table_name}]", conn)
        conn.close()
        if 'IDS' in df.columns: df['IDS'] = df['IDS'].astype(str)
        if 'DATA DE LEITURA' in df.columns: df['DATA DE LEITURA'] = pd.to_datetime(df['DATA DE LEITURA'], errors='coerce').dt.date
        if 'DATA LEITURA' in df.columns: df['DATA LEITURA'] = pd.to_datetime(df['DATA LEITURA'], errors='coerce').dt.date
        numeric_cols = ['LIKES', 'COMENTÁRIOS', 'IDADE DA POSTAGEM', 'ALCANCE', 'SALVOS', 'COMPARTILHAMENTOS', 'VIEWS', 'ENGAJAMENTO', 'SEGUIDORES', 'INTERAÇÕES', 'RESPOSTAS', 'FEMININO', 'MASCULINO', 'INDEFINIDO']
        for col in numeric_cols:
            if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        return df
    except Exception as e:
        logger.exception(f"Erro ao ler tabela {table_name}: {e}")
        return pd.DataFrame()

old_posts = load_old_table('TB_redes_sociais_postagem').rename(columns=DB_COL_MAP_INVERSE)
old_general = load_old_table('TB_redes_sociais_pagina').rename(columns=DB_COL_MAP_INVERSE)
old_gender = load_old_table('TB_redes_sociais_genero').rename(columns=DB_COL_MAP_INVERSE)
old_age = load_old_table('TB_redes_sociais_idade').rename(columns=DB_COL_MAP_INVERSE)
# Cidade combinada; cria dois DFs compatíveis
_old_city = load_old_table('TB_redes_sociais_cidade').rename(columns=DB_COL_MAP_INVERSE)
if not _old_city.empty:
    sel_base = ['PÁGINA', 'CIDADES', 'DATA DE LEITURA']
    old_follower_city = _old_city[sel_base + ['SEGUIDORES']].copy() if 'SEGUIDORES' in _old_city.columns else pd.DataFrame(columns=sel_base+['SEGUIDORES'])
    old_engage_city = _old_city[sel_base + ['ENGAJAMENTO']].copy() if 'ENGAJAMENTO' in _old_city.columns else pd.DataFrame(columns=sel_base+['ENGAJAMENTO'])
else:
    old_follower_city = pd.DataFrame(columns=['PÁGINA', 'CIDADES', 'SEGUIDORES', 'DATA DE LEITURA'])
    old_engage_city = pd.DataFrame(columns=['PÁGINA', 'CIDADES', 'ENGAJAMENTO', 'DATA DE LEITURA'])
old_hashtags = load_old_table('TB_redes_sociais_hashtag')


# --- Loop Principal por Conta ---
accounts = [
    {'ig_username': '@jornal_opopular', 'page_id': '354314593312', 'instagram_account_id': '17841400229391143', 'hashtags': hashtag_list, 'token_env': 'TOKEN_PAGINA_JORNAL_OPOPULAR'},
    {'ig_username': '@tvanhanguera', 'page_id': '107397786018432', 'instagram_account_id': '17841400313145110', 'hashtags': hashtags_anhanguera, 'token_env': 'TOKEN_PAGINA_TVANHANGUERA'},
    {'ig_username': '@jornaldotocantins', 'page_id': '142994199086917', 'instagram_account_id': '17841401937075726', 'hashtags': hashtag_list, 'token_env': 'TOKEN_PAGINA_JORNALDOTOCANTINS'},
    {'ig_username': '@jornaldaqui', 'page_id': '498279863684559', 'instagram_account_id': '17841403024106141', 'hashtags': hashtag_list, 'token_env': 'TOKEN_PAGINA_JORNALDAQUI'},
    {'ig_username': '@radiomoov', 'page_id': '105143742268270', 'instagram_account_id': '17841454085093629', 'hashtags': hashtags_moov, 'token_env': 'TOKEN_PAGINA_RADIOMOOV'},
    {'ig_username': '@jornalismotvanhanguera', 'page_id': '404285082776283', 'instagram_account_id': '17841408026119708', 'hashtags': hashtags_jornalismotv, 'token_env': 'TOKEN_PAGINA_JORNALISMOTVANHANGUERA'},
    {'ig_username': '@guiapratododia', 'page_id': '337997089404200', 'instagram_account_id': '17841466908115773', 'hashtags': hashtags_pratodia, 'token_env': 'TOKEN_PAGINA_GUIAPRATODODIA'},
]

all_new_posts, all_new_general, all_new_gender, all_new_age = [], [], [], []
all_new_engage_city, all_new_follower_city, all_new_tags = [], [], []
all_new_one_day_comments, all_new_seven_days_comments = [], []

MEDIA_FIELDS = 'id,media_type,caption,timestamp,permalink,media_product_type,like_count,comments_count,media_url,thumbnail_url' 
MEDIA_LIMIT = 100 # Reduzido para testes, pode ser aumentado. Idealmente, 50-100 para não sobrecarregar uma única requisição.

for account in accounts:
    logger.info(f"{'='*20} Iniciando processamento para: {account['ig_username']} {'='*20}")

    # Carrega o token específico para a conta
    token = os.getenv(account['token_env'])
    if not token:
        logger.error(f"Token para '{account['ig_username']}' (variável {account['token_env']}) não encontrado no .env. Pulando conta.")
        continue

    # Valida o token antes de prosseguir
    if not check_fb_token_validity(token, account['ig_username']):
        continue # Pula para a próxima conta se o token for inválido

    params = {
        'access_token': token,
        'graph_domain': 'https://graph.facebook.com',
        'graph_version': GRAPH_API_VERSION,
        'endpoint_base': f"https://graph.facebook.com/{GRAPH_API_VERSION}/",
        'page_id': account['page_id'],
        'instagram_account_id': account['instagram_account_id'],
        'ig_username': account['ig_username']
    }
    endpointParams = {'fields': MEDIA_FIELDS, 'limit': str(MEDIA_LIMIT)}
    try:
        (new_posts, new_general, new_gender, new_age, new_engage_city, new_follower_city, new_tags, new_one_day, new_seven_days) = get_data(params, endpointParams, account['hashtags'], processed_stopwords)
        if new_posts is not None and not new_posts.empty: all_new_posts.append(new_posts)
        if new_general is not None and not new_general.empty: all_new_general.append(new_general)
        if new_gender is not None and not new_gender.empty: all_new_gender.append(new_gender)
        if new_age is not None and not new_age.empty: all_new_age.append(new_age)
        if new_engage_city is not None and not new_engage_city.empty: all_new_engage_city.append(new_engage_city)
        if new_follower_city is not None and not new_follower_city.empty: all_new_follower_city.append(new_follower_city)
        if new_tags is not None and not new_tags.empty: all_new_tags.append(new_tags)
        if new_one_day is not None and not new_one_day.empty: all_new_one_day_comments.append(new_one_day) 
        if new_seven_days is not None and not new_seven_days.empty: all_new_seven_days_comments.append(new_seven_days) 
        logger.info(f"Processamento parcial concluído para {account['ig_username']}.")
    except SystemExit: # Captura sys.exit() chamado por funções aninhadas
        logger.error(f"ERRO CRÍTICO: Processamento interrompido para a conta {account['ig_username']} devido a um erro crítico anterior.")
        logger.error(f"Encerrando execução completa do script conforme solicitado.")
        sys.exit(1)
    except Exception as e: 
        logger.exception(f"ERRO CRÍTICO: Erro não tratado durante processamento da conta {account['ig_username']}: {e}")
        logger.error(f"Encerrando execução completa do script para evitar perda de informações.")
        sys.exit(1)
    pause_time = random.uniform(2, 5); logger.info(f"Pausa de {pause_time:.2f} segundos."); time.sleep(pause_time)

# --- Consolidação Final e Salvamento ---
logger.info("\n" + "="*20 + " Iniciando Consolidação Final " + "="*20)

def safe_concat(df_list: List[pd.DataFrame], old_df: pd.DataFrame, drop_duplicates_subset=None) -> pd.DataFrame:
    """Concatena DataFrames novos e antigos de forma segura, removendo duplicatas."""
    if not df_list and (old_df is None or old_df.empty): 
        logger.warning("Nenhum dado novo ou antigo para concatenar."); 
        return pd.DataFrame()
    elif not df_list: 
        logger.info("Nenhum dado novo coletado, usando apenas dados antigos."); 
        return old_df if old_df is not None else pd.DataFrame()
    try:
        ref_cols = None;
        # Encontra colunas de referência do primeiro DataFrame não vazio na lista de novos
        if df_list: 
            first_valid_df = next((df for df in df_list if df is not None and not df.empty), None)
            if first_valid_df is not None:
                ref_cols = first_valid_df.columns
            
        # Se não houver novos dados válidos, mas houver dados antigos, usa as colunas dos antigos
        if ref_cols is None and old_df is not None and not old_df.empty: 
            ref_cols = old_df.columns
            
        # Se ainda não houver colunas de referência (sem dados novos nem antigos), retorna DataFrame vazio
        if ref_cols is None:
            logger.warning("Não foi possível determinar colunas de referência (sem dados novos ou antigos válidos). Retornando DataFrame vazio.")
            return pd.DataFrame()

        aligned_df_list = []
        for df in df_list:
            if df is not None and not df.empty:
                aligned_df_list.append(df.reindex(columns=ref_cols, fill_value=0)) # ou outro fill_value apropriado
            
        if not aligned_df_list and (old_df is None or old_df.empty): # Se não há novos alinhados e não há antigos
            logger.warning("Lista de DFs novos vazia após alinhamento e sem dados antigos. Retornando DataFrame vazio.")
            return pd.DataFrame()
        elif not aligned_df_list: # Se não há novos alinhados, mas há antigos
             logger.info("Lista de DFs novos vazia após alinhamento. Usando apenas dados antigos.")
             return old_df


        new_data_combined = pd.concat(aligned_df_list, ignore_index=True) if aligned_df_list else pd.DataFrame()
        logger.info(f"Dados novos combinados: {len(new_data_combined)} linhas (colunas: {list(new_data_combined.columns) if not new_data_combined.empty else 'N/A'}).")
            
        if old_df is not None and not old_df.empty: 
            final_df = pd.concat([new_data_combined, old_df.reindex(columns=new_data_combined.columns if not new_data_combined.empty else ref_cols, fill_value=0)], ignore_index=True)
        else: 
            final_df = new_data_combined
            
        logger.info(f"Total de {len(final_df)} linhas após concatenar (colunas: {list(final_df.columns) if not final_df.empty else 'N/A'}).")

        if drop_duplicates_subset and not final_df.empty:
            initial_rows = len(final_df);
            # Detecta qual coluna de data existe
            date_col = None
            if 'DATA DE LEITURA' in final_df.columns:
                date_col = 'DATA DE LEITURA'
            elif 'DATA LEITURA' in final_df.columns:
                date_col = 'DATA LEITURA'

            if date_col:
                 final_df[date_col] = pd.to_datetime(final_df[date_col], errors='coerce').dt.date
                 dup_subset_keys = list(drop_duplicates_subset);
                 if date_col not in dup_subset_keys: dup_subset_keys.append(date_col)
                 valid_keys = [key for key in dup_subset_keys if key in final_df.columns]
                 if len(valid_keys) == len(dup_subset_keys):
                     if 'IDS' in valid_keys: final_df['IDS'] = final_df['IDS'].astype(str) # Garante string antes de dropar
                     if date_col in valid_keys: final_df.dropna(subset=[date_col], inplace=True)
                     final_df.drop_duplicates(subset=valid_keys, keep='first', inplace=True)
                     rows_removed = initial_rows - len(final_df)
                     if rows_removed > 0: logger.info(f"Removidas {rows_removed} entradas duplicadas (baseado em {valid_keys}).")
                 else: logger.warning(f"Não foi possível remover duplicatas: Chaves ausentes {set(dup_subset_keys) - set(valid_keys)}")
            else:
                 logger.warning(f"Coluna de data de leitura não encontrada para remover duplicatas. Colunas disponíveis: {list(final_df.columns)}")
        return final_df
    except Exception as e: logger.exception(f"Erro na concatenação segura: {e}."); return old_df if not old_df.empty else pd.DataFrame()

# Consolida cada tipo de DataFrame (normalizando nomes das colunas de data)
# Mapeia para o padrão com espaço (usado internamente no safe_concat): 'DATA DE LEITURA' ou 'DATA LEITURA'
def normalize_date_colnames(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    return df.rename(columns={
        'DATA_DE_LEITURA': 'DATA DE LEITURA',
        'DATA_LEITURA': 'DATA LEITURA'
    })

old_posts = normalize_date_colnames(old_posts)
old_general = normalize_date_colnames(old_general)
old_gender = normalize_date_colnames(old_gender)
old_age = normalize_date_colnames(old_age)
old_follower_city = normalize_date_colnames(old_follower_city)
old_engage_city = normalize_date_colnames(old_engage_city)
old_hashtags = normalize_date_colnames(old_hashtags)

df_all_posts = safe_concat(all_new_posts, old_posts, drop_duplicates_subset=['IDS', 'DATA DE LEITURA'])
df_all_general = safe_concat(all_new_general, old_general, drop_duplicates_subset=['PÁGINA', 'DATA DE LEITURA'])
df_all_gender = safe_concat(all_new_gender, old_gender, drop_duplicates_subset=['PÁGINA', 'TIPO', 'DATA LEITURA'])
df_all_age = safe_concat(all_new_age, old_age, drop_duplicates_subset=['PÁGINA', 'FAIXA DE IDADE', 'DATA DE LEITURA'])
df_all_followers_city = safe_concat(all_new_follower_city, old_follower_city, drop_duplicates_subset=['PÁGINA', 'CIDADES', 'DATA DE LEITURA'])
df_all_engage_city = safe_concat(all_new_engage_city, old_engage_city, drop_duplicates_subset=['PÁGINA', 'CIDADES', 'DATA DE LEITURA'])
df_tag_all = safe_concat(all_new_tags, old_hashtags, drop_duplicates_subset=['IDS', 'HASHTAG', 'DATA DE LEITURA'])

df_all_oneDay = pd.concat(all_new_one_day_comments, ignore_index=True) if all_new_one_day_comments else pd.DataFrame()
df_all_sevenDays = pd.concat(all_new_seven_days_comments, ignore_index=True) if all_new_seven_days_comments else pd.DataFrame()

# Garante que os DataFrames de comentários tenham as colunas esperadas antes de salvar
expected_comment_cols = ['id', 'text', 'timestamp', 'username', 'post_id', 'pagina']
if not df_all_oneDay.empty:
    df_all_oneDay = df_all_oneDay.reindex(columns=expected_comment_cols, fill_value='')
else:
    df_all_oneDay = pd.DataFrame(columns=expected_comment_cols) # Cria vazio com colunas se não houver dados

if not df_all_sevenDays.empty:
    df_all_sevenDays = df_all_sevenDays.reindex(columns=expected_comment_cols, fill_value='')
else:
    df_all_sevenDays = pd.DataFrame(columns=expected_comment_cols)

# Ajusta tipos conforme schema das tabelas de comentários (username é NVARCHAR no DB)
for _df_comments in (df_all_oneDay, df_all_sevenDays):
    if _df_comments is not None and not _df_comments.empty and 'username' in _df_comments.columns:
        # Garante texto; mantém NULL quando ausente
        _df_comments['username'] = _df_comments['username'].apply(lambda v: str(v) if pd.notna(v) else None)


# Verificação final de integridade dos dados consolidados
data_check_passed = True
if df_all_posts.empty:
    logger.warning("ATENÇÃO: DataFrame consolidado de posts está vazio.")
    data_check_passed = False
if df_all_general.empty:
    logger.warning("ATENÇÃO: DataFrame consolidado de dados gerais está vazio.")
    data_check_passed = False

if not data_check_passed:
    logger.warning("ATENÇÃO: Alguns DataFrames consolidados estão vazios. Isso pode indicar problemas na coleta de dados.")
    logger.warning("Verifique se todas as contas foram processadas corretamente.")
else:
    logger.info("VERIFICAÇÃO DE INTEGRIDADE: Todos os DataFrames consolidados contêm dados.")

# Persiste no SQL Server em vez de arquivos
try:
    conn = get_db_connection()

    # Renomeia colunas (espaços -> underscores) conforme schema e converte datas
    df_all_posts = rename_cols_for_db(df_all_posts, DB_COL_MAP_POSTS)
    # Converte coluna HASHTAG de lista -> string (evita erro "Unknown object type list" no upsert)
    if 'HASHTAG' in df_all_posts.columns:
        df_all_posts['HASHTAG'] = df_all_posts['HASHTAG'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
    if 'DATA_DE_LEITURA' in df_all_posts.columns:
        df_all_posts['DATA_DE_LEITURA'] = pd.to_datetime(df_all_posts['DATA_DE_LEITURA'], errors='coerce').dt.date.astype(str)

    df_all_general = rename_cols_for_db(df_all_general, DB_COL_MAP_PAGINA)
    if 'DATA_DE_LEITURA' in df_all_general.columns:
        df_all_general['DATA_DE_LEITURA'] = pd.to_datetime(df_all_general['DATA_DE_LEITURA'], errors='coerce').dt.date.astype(str)

    # Gênero: apenas DATA_LEITURA já está com underscore no DB; se vier com espaço, normalize aqui
    df_all_gender = df_all_gender.rename(columns={'DATA LEITURA': 'DATA_LEITURA'})
    if 'DATA_LEITURA' in df_all_gender.columns:
        df_all_gender['DATA_LEITURA'] = pd.to_datetime(df_all_gender['DATA_LEITURA'], errors='coerce').dt.date.astype(str)

    df_all_age = rename_cols_for_db(df_all_age, DB_COL_MAP_IDADE)
    if 'DATA_DE_LEITURA' in df_all_age.columns:
        df_all_age['DATA_DE_LEITURA'] = pd.to_datetime(df_all_age['DATA_DE_LEITURA'], errors='coerce').dt.date.astype(str)
    # Normaliza métricas de idade para numérico
    for _col in ('ENGAJAMENTO', 'SEGUIDORES'):
        if _col in df_all_age.columns:
            df_all_age[_col] = pd.to_numeric(df_all_age[_col], errors='coerce').fillna(0)

    df_tag_all = rename_cols_for_db(df_tag_all, DB_COL_MAP_HASHTAG)
    if 'DATA_DE_LEITURA' in df_tag_all.columns:
        df_tag_all['DATA_DE_LEITURA'] = pd.to_datetime(df_tag_all['DATA_DE_LEITURA'], errors='coerce').dt.date.astype(str)

    # Cidade combinada e renomeada
    df_city_combined = combine_city_data(df_all_followers_city, df_all_engage_city)
    df_city_combined = rename_cols_for_db(df_city_combined, DB_COL_MAP_CIDADE)
    if 'DATA_DE_LEITURA' in df_city_combined.columns:
        df_city_combined['DATA_DE_LEITURA'] = pd.to_datetime(df_city_combined['DATA_DE_LEITURA'], errors='coerce').dt.date.astype(str)

    # Alinhar colunas ao schema das tabelas antes do upsert
    try:
        df_posts_aligned = align_df_to_table(conn, df_all_posts, 'TB_redes_sociais_postagem', ['IDS', 'DATA_DE_LEITURA'])
        df_general_aligned = align_df_to_table(conn, df_all_general, 'TB_redes_sociais_pagina', ['PÁGINA', 'DATA_DE_LEITURA'])
        df_gender_aligned = align_df_to_table(conn, df_all_gender, 'TB_redes_sociais_genero', ['PÁGINA', 'TIPO', 'DATA_LEITURA'])
        df_age_aligned = align_df_to_table(conn, df_all_age, 'TB_redes_sociais_idade', ['PÁGINA', 'FAIXA_DE_IDADE', 'DATA_DE_LEITURA'])
        df_city_aligned = align_df_to_table(conn, df_city_combined, 'TB_redes_sociais_cidade', ['PÁGINA', 'CIDADES', 'DATA_DE_LEITURA'])
        df_tags_aligned = align_df_to_table(conn, df_tag_all, 'TB_redes_sociais_hashtag', ['IDS', 'HASHTAG', 'DATA_DE_LEITURA'])
        df_comments_day_aligned = align_df_to_table(conn, df_all_oneDay, 'TB_redes_sociais_comentario_dia', ['id'])
        df_comments_week_aligned = align_df_to_table(conn, df_all_sevenDays, 'TB_redes_sociais_comentario_semana', ['id'])
    except KeyError as e:
        logger.exception(f"Coluna obrigatória ausente para upsert: {e}")
        raise

    # Upserts
    upsert_dataframe(conn, df_posts_aligned, 'TB_redes_sociais_postagem', ['IDS', 'DATA_DE_LEITURA'])
    upsert_dataframe(conn, df_general_aligned, 'TB_redes_sociais_pagina', ['PÁGINA', 'DATA_DE_LEITURA'])
    upsert_dataframe(conn, df_gender_aligned, 'TB_redes_sociais_genero', ['PÁGINA', 'TIPO', 'DATA_LEITURA'])
    upsert_dataframe(conn, df_age_aligned, 'TB_redes_sociais_idade', ['PÁGINA', 'FAIXA_DE_IDADE', 'DATA_DE_LEITURA'])
    upsert_dataframe(conn, df_city_aligned, 'TB_redes_sociais_cidade', ['PÁGINA', 'CIDADES', 'DATA_DE_LEITURA'])
    upsert_dataframe(conn, df_tags_aligned, 'TB_redes_sociais_hashtag', ['IDS', 'HASHTAG', 'DATA_DE_LEITURA'])
    # Comentários: apagar e regravar SOMENTE se houver linhas novas
    if df_comments_day_aligned is not None and not df_comments_day_aligned.empty:
        if has_new_rows(conn, 'TB_redes_sociais_comentario_dia', df_comments_day_aligned, key_col='id'):
            if can_delete(conn, 'TB_redes_sociais_comentario_dia'):
                delete_all_rows(conn, 'TB_redes_sociais_comentario_dia')
                upsert_dataframe(conn, df_comments_day_aligned, 'TB_redes_sociais_comentario_dia', ['id'])
            else:
                raise PermissionError("Sem permissão de DELETE na tabela TB_redes_sociais_comentario_dia para sobrescrever comentários de 1 dia.")
        else:
            logger.info('Sem novos comentários de 1 dia; não houve sobrescrita.')
    if df_comments_week_aligned is not None and not df_comments_week_aligned.empty:
        if has_new_rows(conn, 'TB_redes_sociais_comentario_semana', df_comments_week_aligned, key_col='id'):
            if can_delete(conn, 'TB_redes_sociais_comentario_semana'):
                delete_all_rows(conn, 'TB_redes_sociais_comentario_semana')
                upsert_dataframe(conn, df_comments_week_aligned, 'TB_redes_sociais_comentario_semana', ['id'])
            else:
                raise PermissionError("Sem permissão de DELETE na tabela TB_redes_sociais_comentario_semana para sobrescrever comentários de 7 dias.")
        else:
            logger.info('Sem novos comentários de 7 dias; não houve sobrescrita.')

    conn.close()
    logger.info("SUCESSO: Dados gravados no SQL Server em todas as tabelas alvo.")
except Exception as e: 
    logger.exception(f"ERRO CRÍTICO: Falha ao gravar dados no SQL Server: {e}")
    sys.exit(1)

logger.info("\n" + "="*20 + " Script Concluído com Sucesso " + "="*20)
logger.info("EXECUÇÃO FINALIZADA: Todos os dados foram processados e salvos corretamente.")
logger.info("Nenhum erro crítico foi encontrado durante a execução.")
