from curl_cffi import requests as cffi_requests
import requests as standard_requests
import json
from datetime import datetime, date, timedelta
import time
import os
from pathlib import Path
import urllib.parse
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import threading
import pandas as pd
import sys
import unicodedata
import re
import sqlite3

# --- LÓGICA DE CAMINHOS AUTOMÁTICOS ---
PASTA_DO_SCRIPT = Path(__file__).resolve().parent

try:
    from playsound import playsound
    SOUND_ENABLED = True
except ImportError:
    print("[AVISO] Biblioteca 'playsound' não encontrada. O alerta será visual.")
    SOUND_ENABLED = False


class Cores:
    VERMELHO = "\033[91m"
    AMARELO = "\033[93m"
    VERDE = "\033[92m"
    CIANO = "\033[96m"
    ROXO = "\033[95m"
    RESET = "\033[0m"
    BRANCO = "\033[97m"

# =========================================================================
# === CLASSE DE BANCO DE DADOS (SQLITE) ===
# =========================================================================
class BancoDados:
    def __init__(self):
        self.path = PASTA_DO_SCRIPT / "historico_monitor.db"
        self.conn = None
        self.conectar()
        self.criar_tabelas()

    def conectar(self):
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.cursor = self.conn.cursor()

    def criar_tabelas(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS quedas_massivas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data_registro DATETIME,
                cidade TEXT,
                bairro TEXT,
                qtd_afetados INTEGER,
                status TEXT
            )
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS clientes_afetados (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                id_evento_queda INTEGER,
                id_cliente INTEGER,
                login TEXT,
                nome TEXT,
                endereco TEXT,
                data_registro DATETIME,
                FOREIGN KEY(id_evento_queda) REFERENCES quedas_massivas(id)
            )
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS retrabalhos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                id_os TEXT UNIQUE,
                data_abertura TEXT,
                cliente TEXT,
                tipo_os TEXT,
                tecnico TEXT, 
                empresa TEXT,
                cidade TEXT,
                tipo_os_detalhe TEXT,
                data_registro DATETIME
            )
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS reagendamentos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                id_os TEXT UNIQUE,
                cliente TEXT,
                cidade TEXT,
                status TEXT,
                data_registro DATETIME
            )
        """)
        self.conn.commit()

    def verificar_duplicidade_queda(self, cidade, bairro):
        try:
            limite_tempo = (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d %H:%M:%S")
            
            self.cursor.execute("""
                SELECT id 
                FROM quedas_massivas 
                WHERE cidade = ? 
                AND bairro = ? 
                AND (status = 'DETECTADO' OR data_registro >= ?)
                LIMIT 1
            """, (cidade, bairro, limite_tempo))
            
            resultado = self.cursor.fetchone()
            if resultado:
                return True
            return False
        except Exception as e:
            print(f"Erro check duplicidade: {e}")
            return False

    def registrar_queda(self, cidade, bairro, qtd, lista_clientes):
        try:    
            agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.cursor.execute("""
                INSERT INTO quedas_massivas (data_registro, cidade, bairro, qtd_afetados, status)
                VALUES (?, ?, ?, ?, ?)
            """, (agora, cidade, bairro, qtd, 'DETECTADO'))
            
            id_evento = self.cursor.lastrowid
            for _, row in lista_clientes.iterrows():
                id_cli = row.get('id_cliente', 0)
                login = str(row.get('login', ''))
                nome = str(row.get('cliente', ''))
                end = str(row.get('endereco', ''))
                
                self.cursor.execute("""
                    INSERT INTO clientes_afetados (id_evento_queda, id_cliente, login, nome, endereco, data_registro)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (id_evento, id_cli, login, nome, end, agora))
            
            self.conn.commit()
        except Exception as e:
            print(f"{Cores.VERMELHO}[BD Erro Ao Salvar Queda] {e}{Cores.RESET}")

    def atualizar_status_queda(self, cidade, bairro, novo_status='NORMALIZADO'):
        try:
            self.cursor.execute("""
                UPDATE quedas_massivas
                SET status = ?
                WHERE cidade = ? 
                AND bairro = ? 
                AND status = 'DETECTADO'
            """, (novo_status, cidade, bairro))
            self.conn.commit()
        except Exception as e:
            print(f"{Cores.VERMELHO}[Erro BD Atualizar Queda] {e}{Cores.RESET}")

    def registrar_retrabalho(self, row_data, cols=None):
        try:
            id_os = row_data[2] if len(row_data) > 2 else row_data[0]
            data_abertura = row_data[1]
            cliente_raw = str(row_data[0]) 
            cliente = cliente_raw
            tipo_os_detalhe = row_data[3] if len(row_data) > 3 else "N/D"
            empresa = "N/D"
            tecnico = "N/D"
            cidade_real = REGIONAL_ATUAL_NOME

            if cols:
                col_names_lower = [c.get('name', '').lower() for c in cols]
                idx_tec = -1
                termos_tec = ['usuario_referente', 'usuario', 'tecnico', 'executado', 'responsavel']
                
                for termo in termos_tec:
                    for i, name in enumerate(col_names_lower):
                        if termo in name:
                            idx_tec = i
                            break
                    if idx_tec != -1: break
                
                if idx_tec != -1 and len(row_data) > idx_tec:
                    val_t = row_data[idx_tec]
                    if val_t:
                        texto_completo = str(val_t).strip().upper()
                        if "|" in texto_completo:
                            partes = texto_completo.split("|")
                            tecnico = partes[0].strip() 
                            if len(partes) > 1:
                                empresa = partes[1].strip() 
                        else:
                            tecnico = texto_completo
                idx_cid = -1
                termos_cid = ['cidade', 'municipio', 'localidade']
                for termo in termos_cid:
                    for i, name in enumerate(col_names_lower):
                        if termo in name:
                            idx_cid = i
                            break
                    if idx_cid != -1: break
                
                if idx_cid != -1 and len(row_data) > idx_cid:
                    val_c = row_data[idx_cid]
                    if val_c: cidade_real = str(val_c).strip().upper()
            agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            self.cursor.execute("""
                INSERT INTO retrabalhos (id_os, data_abertura, cliente, tecnico, empresa, cidade, tipo_os_detalhe, data_registro)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id_os) DO UPDATE SET tecnico=excluded.tecnico, empresa=excluded.empresa, cidade=excluded.cidade
            """, (str(id_os), str(data_abertura), str(cliente), str(tecnico), str(empresa), str(cidade_real), str(tipo_os_detalhe), agora))
            self.conn.commit()
            
        except Exception as e:
            print(f"{Cores.VERMELHO}[ERRO DB RETRABALHO] {e}{Cores.RESET}")

    def registrar_reagendamento(self, row_data, cols):
        try:
            col_names = [c["name"] for c in cols]
            idx_os = col_names.index(CONFIG_COLUNAS["NOME_COLUNA_OS"])
            idx_cli = col_names.index(CONFIG_COLUNAS["NOME_COLUNA_CLIENTE"])
            idx_cid = col_names.index(CONFIG_COLUNAS["NOME_COLUNA_CIDADE"])
            id_os = row_data[idx_os]
            cliente = row_data[idx_cli]
            cidade = row_data[idx_cid]
            agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.cursor.execute("""
                INSERT OR IGNORE INTO reagendamentos (id_os, cliente, cidade, status, data_registro)
                VALUES (?, ?, ?, ?, ?)
            """, (str(id_os), str(cliente), str(cidade), 'AGUARDANDO_AGENDAMENTO', agora))
            self.conn.commit()
        except Exception as e:
            print(f"{Cores.VERMELHO}[ERRO DB AGENDAMENTO] {e}{Cores.RESET}")

    def limpar_banco(self):
        try:
            self.cursor.execute("DELETE FROM quedas_massivas")
            self.cursor.execute("DELETE FROM clientes_afetados")
            self.cursor.execute("DELETE FROM retrabalhos")
            self.cursor.execute("DELETE FROM reagendamentos")
            self.cursor.execute("DELETE FROM sqlite_sequence")
            self.conn.commit()
            self.cursor.execute("VACUUM")
            return True
        except Exception as e:
            print(f"Erro ao limpar: {e}")
            return False

bd = BancoDados()
NOME_ARQUIVO_CONFIG = PASTA_DO_SCRIPT / "config_monitor.json"
NOME_ARQUIVO_LOG = PASTA_DO_SCRIPT / "log_alertas.txt"
PASTA_DE_SONS = PASTA_DO_SCRIPT / "sons"
PASTA_WHATSAPP = PASTA_DO_SCRIPT / "whatsapp"
PASTA_QUEDAS = PASTA_DO_SCRIPT / "Quedas"
PASTA_INFO = PASTA_DO_SCRIPT / "info"  

TIMEOUT_SEGUNDOS = 90
INTERVALO_MONITORAMENTO_PADRAO = 30
MINIMO_PARA_ALERTA_QUEDA = 20

# === CONFIGURAÇÕES DO MAPA ===
HEADERS_MAPA = {
    "Authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6IjFiOTQzNzYwMjc3NjNlZjQxYzc2Y2ZiMzIzYjk1MTM1OTU0MzM2MzNkOGY3Y2Q5N2VjMmVkZmU0YTU0MjE5ZDNiZWRhNTg3OTFhY2QyM2FmIn0.eyJhdWQiOiIxIiwianRpIjoiMWI5NDM3NjAyNzc2M2VmNDFjNzZjZmIzMjNiOTUxMzU5NTQzMzYzM2Q4ZjdjZDk3ZWMyZWRmZTRhNTQyMTlkM2JlZGE1ODc5MWFjZDIzYWYiLCJpYXQiOjE3NTk2MzI2NjksIm5iZiI6MTc1OTYzMjY2OSwiZXhwIjoxOTE3Mzk5MDY4LCJzdWIiOiI0NTA4Iiwic2NvcGVzIjpbXX0.Jn7zbjCWNN-oJueDe3LpJj4t2Y1X2FWhcb5TXjr0IcIhwyegGwXCvUR24NEeii93JHQrpJpjq6Auz6Gh8q7SnCKeaYAdduBPj88RaR4YxXkazfyMA3xWQvpxFc5Sd2NJkVduGnErzP2gL1w1XUqUhJT6KbUbYL0XQ1Q1iTtUXoiaM5rPf6sZApA7AyjcQF_rBSHKaF3gu-VBeffeLwXHTZYAHRV8eBHrCOtxmr1FXzvvakld2q9ui1iFj6-yQ_cMqu8wDJUy469mQCjlhQUGnghwPmyesuBsk6KzplSQqI2Cw8TA_89ov-dBJ2GIC99MjaAYw8WmV2reyS_iV8AOvCnpNHs6PY5Amoyaj7J2ey_eZU_pLiMDQL59PsOw3kiI4BAQVB5yeeswhKFPVv7GBKpIb0bxKUZ9YbIFvmeX2DBZdwNGDjNdZXm65V7mX8BwixY-dxaGLs91_6jClWeFeOk9ECIC3E0QU_zyevJoho0-CGkL7830WEDoNePxy3_wgCQQ8NB-VjKemXoHoX2zrI-K3lLVasUcXVICy0NoMAHBmpIS_2X5UBJ5DijFbfuAA3BzUrrpdvgu1kwQ1dy8vto2LODfuW4MwWhyjHMZg8Av9r3SkQNac1NEhWi7PiuNKhuzTdWFcQ4fAS4Fg0Q_xhjSnfucAHC_rh9QOUykcjA",
    "Content-Type": "application/json;charset=UTF-8",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
}
URL_API_MAPA = "https://api.sempre.hubsoft.com.br/api/v1/mapeamento/cliente/carregar"

LISTA_METROPOLITANA_SUB_2 = [
    {"id_cidade": 1443, "id_estado": 13, "nome": "Brumadinho", "display": "BRUMADINHO/MG"},
    {"id_cidade": 2115, "id_estado": 13, "nome": "Sarzedo", "display": "SARZEDO/MG"},
    {"id_cidade": 1683, "id_estado": 13, "nome": "Ibirite", "display": "IBIRITE/MG"},
    {"id_cidade": 1414, "id_estado": 13, "nome": "Belo Vale", "display": "BELO VALE/MG"},
    {"id_cidade": 1834, "id_estado": 13, "nome": "Moeda", "display": "MOEDA/MG"},
    {"id_cidade": 1989, "id_estado": 13, "nome": "Rio Manso", "display": "RIO MANSO/MG"},
    {"id_cidade": 1935, "id_estado": 13, "nome": "Piedade dos Gerais", "display": "PIEDADE DOS GERAIS/MG"},
    {"id_cidade": 1807, "id_estado": 13, "nome": "Mario Campos", "display": "MARIO CAMPOS/MG"},
    {"id_cidade": 1865, "id_estado": 13, "nome": "Nova Lima", "display": "NOVA LIMA/MG"},
    {"id_cidade": 1708, "id_estado": 13, "nome": "Itabirito", "display": "ITABIRITO/MG"}
]

# === CONFIGURAÇÕES DO DASHBOARD ===
COOKIES = {
    "_gid": "GA1.3.763926516.1759799203",
    "_ga_QL4S9YYYLP": "GS2.1.s1759799203$o1$g0$t1759799203$j60$l0$h0",
    "_ga": "GA1.1.1224821756.1759799203",
    "metabase.DEVICE": "d04cbcba-6ff6-41b9-a5f7-f60af33db988",
}
HEADERS = {
    "Accept": "application/json",
    "Referer": "https://bi.sempre.hubsoft.com.br:8443/public/dashboard/bd1af1ee-2049-4ab9-b386-ecf6c8dac854",
}
SONS_DE_ALERTA = {
    "RETRABALHO": PASTA_DE_SONS / "teste.mp3",
    "OS_AGENDAMENTO": PASTA_DE_SONS / "alerta_os.mp3",
    "OS_AGENDAMENTO_MASSA": PASTA_DE_SONS / "ratinho.mp3",
    "QUEDA_MASSIVA": PASTA_DE_SONS / "sirene.mp3"
}

# --- VARIÁVEIS GLOBAIS ---
CONFIG_REGIONAIS = {}
CONFIG_COLUNAS = {}
CONFIG_OS_TIPOS = {}
EMAIL_SENDER_SETTINGS = {}
TELEGRAM_SETTINGS = {}
REGIONAL_ATUAL_NOME = ""
config_atual = {}
REGIONAL_ATUAL_VALOR_API = ""
FILTRO_EMPRESAS_RETRABALHO = []
CIDADES_DA_REGIONAL_ATUAL = []
historico_quedas_mapa = {}

# --- URLs BASE DASHBOARD ---
ID_FILTRO_REGIONAL = "bde1e635"
API_URL_RETRABALHO = "https://bi.sempre.hubsoft.com.br:8443/api/public/dashboard/bd1af1ee-2049-4ab9-b386-ecf6c8dac854/dashcard/3112/card/2716?parameters=%5B%5D"
BASE_URL_OS_SUPORTE = "https://bi.sempre.hubsoft.com.br:8443/api/public/dashboard/bd1af1ee-2049-4ab9-b386-ecf6c8dac854/dashcard/2671/card/2307"
BASE_URL_OS_MUDANCA = "https://bi.sempre.hubsoft.com.br:8443/api/public/dashboard/bd1af1ee-2049-4ab9-b386-ecf6c8dac854/dashcard/2670/card/2312"
STATUS_ALVO_OS = "aguardando_agendamento"
STATUS_SLA_VENCIDO = "Atrasado"
def remover_acentos(texto):
    if not isinstance(texto, str): return ""
    return ''.join(c for c in unicodedata.normalize('NFD', texto)
                  if unicodedata.category(c) != 'Mn').upper()

def len_visivel(texto):
    """Retorna o tamanho do texto ignorando códigos de cor ANSI."""
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return len(ansi_escape.sub('', texto))

def remover_ansi(texto):
    """Remove códigos de cor ANSI de uma string."""
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', texto)

def obter_coordenadas(cidade):
    """
    Busca Lat/Lon da cidade usando Open-Meteo e Curl_CFFI para evitar bloqueios.
    Tenta ser específico primeiro, depois genérico.
    """
    cidade_limpa = cidade.split('-')[0].strip()
    termos_busca = [
        f"{cidade_limpa}, Minas Gerais",  
        cidade_limpa                      
    ]

    for tentativa in termos_busca:
        try:
            url = f"https://geocoding-api.open-meteo.com/v1/search?name={urllib.parse.quote(tentativa)}&count=10&language=pt&format=json"
            
            response = cffi_requests.get(url, timeout=10, impersonate="chrome110")
            
            if response.status_code != 200:
                continue 

            data = response.json()
            
            if 'results' in data and data['results']:
                candidatos = data['results']
                melhor_match = None
                
                for item in candidatos:
                    pais = item.get('country', '').upper()
                    estado = item.get('admin1', '').upper()
                    
                    if 'BRAZIL' in pais or 'BRASIL' in pais:
                        melhor_match = item
                        if 'MINAS' in estado or 'MG' in estado:
                            return item['latitude'], item['longitude']
                if melhor_match:
                    return melhor_match['latitude'], melhor_match['longitude']

        except Exception as e:
            pass
    print(f"\n[Aviso] Cidade não encontrada nem com busca genérica: {cidade_limpa}")
    return None, None

def menu_previsao_chuva():
    print(f"\n{Cores.CIANO}🌤️  Consultando Previsão do Tempo (5 Dias)...{Cores.RESET}")
    print(f"Regional: {REGIONAL_ATUAL_NOME}")
    
    if not CIDADES_DA_REGIONAL_ATUAL:
        print(f"{Cores.VERMELHO}Erro: Nenhuma cidade configurada nesta regional.{Cores.RESET}")
        return

    hoje = datetime.now()
    cabecalhos = ["CIDADE"]
    datas_formatadas = []
    for i in range(5):
        data_futura = hoje + timedelta(days=i)
        str_data = data_futura.strftime("%d/%m") 
        cabecalhos.append(str_data)
        datas_formatadas.append(str_data)

    buffer_relatorio = []
    buffer_relatorio.append(f"🌦️ *PREVISÃO DE CHUVA - {REGIONAL_ATUAL_NOME.upper()}*")
    buffer_relatorio.append(f"Gerado em: {hoje.strftime('%d/%m/%Y %H:%M')}")
    buffer_relatorio.append("-" * 40)
    dados_tabela = []
    total_cidades = len(CIDADES_DA_REGIONAL_ATUAL)
    headers_api = {"User-Agent": "MonitoramentoRegional/1.0"}

    for i, cidade in enumerate(CIDADES_DA_REGIONAL_ATUAL, 1):
        sys.stdout.write(f"\rProcessando {i}/{total_cidades}: {cidade}...")
        sys.stdout.flush()
        
        lat, lon = obter_coordenadas(cidade)
        
        if not lat:
            linha_erro = [cidade] + ["N/A"] * 5
            dados_tabela.append(linha_erro)
            continue
        try:
            url_weather = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&daily=precipitation_sum,precipitation_probability_max&timezone=America%2FSao_Paulo"
            res = standard_requests.get(url_weather, headers=headers_api, timeout=10).json()
            
            daily = res.get('daily', {})
            probs = daily.get('precipitation_probability_max', [])
            mm = daily.get('precipitation_sum', [])
            
            linha_visual = [cidade]
            linha_txt = f"📍 *{cidade.upper()}*:\n"
            for d in range(5):
                try:
                    data_coluna = datas_formatadas[d]
                    prob = probs[d]
                    qtd = mm[d]
                    icone = "☀️"
                    cor_celula = Cores.VERDE
                    
                    if prob >= 30: 
                        icone = "⛅"
                        cor_celula = Cores.AMARELO
                    if prob >= 60: 
                        icone = "🌧️"
                        cor_celula = Cores.CIANO
                    if prob >= 80: 
                        icone = "⛈️"
                        cor_celula = Cores.VERMELHO
                    str_celula = f"{prob}% ({qtd}mm)"
                    linha_visual.append(str_celula)
                    linha_txt += f"   🗓️ {data_coluna}: {icone} {prob}% ({qtd}mm)\n"
                except IndexError:
                    linha_visual.append("-")
            
            dados_tabela.append(linha_visual)
            buffer_relatorio.append(linha_txt)
            buffer_relatorio.append("-" * 20)
            
        except Exception as e:
            dados_tabela.append([cidade, "Erro API", "-", "-", "-", "-"])

    sys.stdout.write(f"\rConcluído!{' '*20}\n")
    print("\n")
    imprimir_tabela_bonita(cabecalhos, dados_tabela, cor_borda=Cores.CIANO, cor_texto=Cores.BRANCO)
    print(f"\n{Cores.AMARELO}Deseja salvar este relatório para WhatsApp? (s/n){Cores.RESET}")
    if input("Opção: ").lower().strip() == 's':
        nome_arquivo = PASTA_INFO / f"PREVISAO_CHUVA_{REGIONAL_ATUAL_NOME}_{datetime.now().strftime('%Y-%m-%d')}.txt"
        try:
            with open(nome_arquivo, "w", encoding="utf-8") as f:
                f.write("\n".join(buffer_relatorio))
            print(f"{Cores.VERDE}✅ Salvo em: {nome_arquivo.name}{Cores.RESET}")
        except Exception as e:
            print(f"Erro ao salvar: {e}")

def relatorio_risco_sla():
    print(f"\n{Cores.AMARELO}⚠️  ANÁLISE DE RISCO DE SLA (Próximas 4h){Cores.RESET}")
    print(f"Regional: {REGIONAL_ATUAL_NOME}")
    
    rows, cols = buscar_dados(BASE_URL_OS_SUPORTE)
    if not rows: 
        print("Sem dados na API de Suporte.")
        return
    col_names = [c["name"].lower() for c in cols]
    col_displays = [c.get("display_name", "").lower() for c in cols]
    
    idx_tempo = -1
    termos_sla = ['restante', 'tempo', 'sla', 'atraso', 'vencimento']
    
    for i, nome in enumerate(col_names):
        if any(t in nome for t in termos_sla): idx_tempo = i; break
    if idx_tempo == -1:
        for i, disp in enumerate(col_displays):
            if any(t in disp for t in termos_sla): idx_tempo = i; break
            
    if idx_tempo == -1:
        print(f"{Cores.VERMELHO}Erro: Coluna de tempo/SLA não encontrada.{Cores.RESET}") 
        return

    idx_os = next((i for i, n in enumerate(col_names) if 'ordem' in n or 'os' in n), 0)
    idx_cli = next((i for i, n in enumerate(col_names) if 'cliente' in n or 'parceiro' in n), 1)
    idx_cid = -1
    if CIDADES_DA_REGIONAL_ATUAL:
        for i, nome in enumerate(col_names):
            if 'cidade' in nome or 'municipio' in nome: idx_cid = i; break

    buffer = []
    buffer.append(f"🚨 *RISCO IMINENTE DE SLA (< 4 Horas)*")
    buffer.append(f"Regional: {REGIONAL_ATUAL_NOME}")
    buffer.append("-" * 40)

    stats = {
        "analisados": 0,
        "ignorados_outra_regional": 0,
        "ja_vencidos": 0,
        "prazo_longo": 0,
        "em_risco": 0,
        "erro_leitura": 0
    }
    
    cidades_perm = [c.upper().strip() for c in CIDADES_DA_REGIONAL_ATUAL] if CIDADES_DA_REGIONAL_ATUAL else []

    for r in rows:
        if len(r) <= idx_tempo: continue
        stats["analisados"] += 1
        if idx_cid != -1 and cidades_perm:
            cidade_row = str(r[idx_cid]).upper().strip()
            if cidade_row not in cidades_perm:
                stats["ignorados_outra_regional"] += 1
                continue

        tempo_str = str(r[idx_tempo]).lower().strip()
        if "-" in tempo_str or "atrasado" in tempo_str:
            stats["ja_vencidos"] += 1
            continue

        try:
            match_dias = re.search(r'(\d+)\s*dia', tempo_str)
            dias = int(match_dias.group(1)) if match_dias else 0
            if dias > 0:
                stats["prazo_longo"] += 1
                continue
            match_horas = re.search(r'(\d{1,2}):(\d{2})', tempo_str)
            
            if match_horas:
                horas = int(match_horas.group(1))
                if horas < 4:
                    stats["em_risco"] += 1
                    os_val = r[idx_os]
                    cli_val = r[idx_cli]
                    tempo_limpo = tempo_str.replace("0 dias e ", "").replace("horas", "").strip()
                    buffer.append(f"🔸 OS {os_val} | {cli_val}")
                    buffer.append(f"   ⏳ Restam: {Cores.VERMELHO}{tempo_limpo}{Cores.RESET}")
                else:
                    stats["prazo_longo"] += 1
            else:
                stats["erro_leitura"] += 1

        except Exception:
            stats["erro_leitura"] += 1
    if stats["em_risco"] > 0:
        print(f"\n{Cores.VERMELHO}⚠️  ENCONTRADAS {stats['em_risco']} O.S. EM RISCO!{Cores.RESET}")
        for linha in buffer[3:]:
            print(linha)
    else:
        print(f"\n{Cores.VERDE}✅ Nenhuma O.S. em Risco Iminente (<4h){Cores.RESET}")
    print(f"\n{Cores.CIANO}--- RESUMO DA ANÁLISE ---{Cores.RESET}")
    print(f"Total Analisado na API: {stats['analisados']}")
    print(f"Ignorados (Outra Regional): {stats['ignorados_outra_regional']}")
    print(f"Já Vencidos (Negativos): {Cores.VERMELHO}{stats['ja_vencidos']}{Cores.RESET}")
    print(f"Prazo Seguro (>4h ou >1 dia): {Cores.VERDE}{stats['prazo_longo']}{Cores.RESET}")
    print(f"Em Risco (0 dias e <4h): {Cores.AMARELO}{stats['em_risco']}{Cores.RESET}")
    
    if stats["em_risco"] > 0:
        print(f"\n{Cores.AMARELO}Deseja salvar a lista de risco? (s/n){Cores.RESET}")
        if input("Opção: ").lower() == 's':
            buffer_limpo = [remover_ansi(b) for b in buffer]
            exportar_relatorio("Risco_SLA", buffer_limpo)

def gerar_morning_call():
    print(f"\n{Cores.ROXO}☕ GERANDO MORNING CALL... (Aguarde){Cores.RESET}")
    
    hoje = datetime.now().strftime("%d/%m/%Y")
    ontem_data = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    buffer = []
    buffer.append(f"☕ *BOM DIA, EQUIPE {REGIONAL_ATUAL_NOME}*")
    buffer.append(f"📅 Resumo para: {hoje}")
    buffer.append("="*30)

    print(f"1/4 Analisando Histórico de Ontem... ", end="", flush=True)
    try:
        bd.cursor.execute("SELECT COUNT(*), SUM(qtd_afetados) FROM quedas_massivas WHERE date(data_registro) = ?", (ontem_data,))
        res_ontem = bd.cursor.fetchone()
        qtd_eventos = res_ontem[0] or 0
        qtd_cli = res_ontem[1] or 0
        buffer.append(f"📉 *Retrospectiva (Ontem):*")
        buffer.append(f"   • Eventos Massivos: {qtd_eventos}")
        buffer.append(f"   • Clientes Afetados: {qtd_cli}")
        print(f"{Cores.VERDE}OK{Cores.RESET}")
    except Exception as e:
        print(f"{Cores.VERMELHO}Erro{Cores.RESET}")
        buffer.append(f"   (Erro ao ler histórico: {e})")

    try:
        bd.cursor.execute("SELECT COUNT(*) FROM quedas_massivas WHERE status = 'DETECTADO'")
        ativas = bd.cursor.fetchone()[0]
        icone_status = "✅" if ativas == 0 else "🔥"
        buffer.append(f"\n{icone_status} *Status Rede Agora:*")
        if ativas == 0:
            buffer.append("   • Rede Estável. Sem massivas.")
        else:
            buffer.append(f"   • {ativas} massiva(s) ativa(s).")
    except:
        buffer.append("   (Erro ao ler status atual)")

    print(f"2/4 Buscando Retrabalhos (Filtro Empresa)... ", end="", flush=True)
    total_retrabalho = 0
    try:
        rows_rw, _ = buscar_dados(API_URL_RETRABALHO)
        if rows_rw:
            empresas_perm = [e.lower().strip() for e in FILTRO_EMPRESAS_RETRABALHO]
            
            if not empresas_perm:
                total_retrabalho = "Erro (Lista Empresas Vazia)"
            else:
                for r in rows_rw:
                    linha_str = str(r).lower()
                    if any(emp in linha_str for emp in empresas_perm):
                        total_retrabalho += 1
                        
        print(f"{Cores.VERDE}OK ({total_retrabalho}){Cores.RESET}")
    except Exception as e:
        print(f"{Cores.VERMELHO}Erro{Cores.RESET}")
        total_retrabalho = "Erro"
    
    buffer.append(f"\n🛠️ *Retrabalhos na Fila:* {total_retrabalho}")

    print(f"3/4 Contabilizando O.S. por Cidade... ", end="", flush=True)
    stats_cidades = {c.upper().strip(): {'sup': 0, 'inst': 0} for c in CIDADES_DA_REGIONAL_ATUAL}
    
    try:
        rows_sup, cols_sup = buscar_dados(BASE_URL_OS_SUPORTE)
        if rows_sup:
            idx_c = next((i for i, c in enumerate(cols_sup) if 'cidade' in c['name'].lower() or 'municipio' in c['name'].lower()), -1)
            if idx_c != -1:
                for r in rows_sup:
                    if len(r) > idx_c:
                        cidade = str(r[idx_c]).upper().strip()
                        if cidade in stats_cidades: stats_cidades[cidade]['sup'] += 1

        rows_inst, cols_inst = buscar_dados(BASE_URL_OS_MUDANCA)
        if rows_inst:
            idx_c = next((i for i, c in enumerate(cols_inst) if 'cidade' in c['name'].lower() or 'municipio' in c['name'].lower()), -1)
            if idx_c != -1:
                for r in rows_inst:
                    if len(r) > idx_c:
                        cidade = str(r[idx_c]).upper().strip()
                        if cidade in stats_cidades: stats_cidades[cidade]['inst'] += 1
        print(f"{Cores.VERDE}OK{Cores.RESET}")
    except:
        print(f"{Cores.VERMELHO}Erro{Cores.RESET}")

    buffer.append(f"\n📊 *O.S. em Aberto (Por Cidade):*")
    tem_os = False
    for cidade in CIDADES_DA_REGIONAL_ATUAL:
        cid_key = cidade.upper().strip()
        s = stats_cidades.get(cid_key, {'sup': 0, 'inst': 0})
        if s['sup'] > 0 or s['inst'] > 0:
            tem_os = True
            buffer.append(f"   • {cidade}: {s['sup']} Sup | {s['inst']} Inst")
    
    if not tem_os: buffer.append("   (Nenhuma O.S. pendente)")
    print(f"4/4 Verificando Clima... ")
    buffer.append(f"\n🌦️ *Previsão Hoje:*")
    headers_api = {"User-Agent": "MorningCall/1.0"}
    
    total_cidades = len(CIDADES_DA_REGIONAL_ATUAL)
    for i, cidade in enumerate(CIDADES_DA_REGIONAL_ATUAL, 1):
        sys.stdout.write(f"\r   -> {i}/{total_cidades}: {cidade}    ")
        sys.stdout.flush()
        
        lat, lon = obter_coordenadas(cidade)
        if lat:
            try:
                url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&daily=precipitation_probability_max&timezone=America%2FSao_Paulo"
                r = cffi_requests.get(url, headers=headers_api, impersonate="chrome110", timeout=3).json()
                prob = r['daily']['precipitation_probability_max'][0]
                
                icone = "☀️"
                if prob >= 30: icone = "⛅"
                if prob >= 60: icone = "🌧️"
                
                buffer.append(f"   • {cidade}: {icone} {prob}%")
            except: 
                buffer.append(f"   • {cidade}: (Erro API)")
        else:
            buffer.append(f"   • {cidade}: (Não achada)")
            
    buffer.append("="*30)
    buffer.append("Bom trabalho a todos!")
    
    print(f"\n\n{Cores.VERDE}=== RELATÓRIO GERADO ==={Cores.RESET}")
    for linha in buffer:
        print(linha)
    
    print(f"\n{Cores.AMARELO}Deseja salvar este Morning Call em TXT? (s/n){Cores.RESET}")
    if input("Opção: ").lower().strip() == 's':
        exportar_relatorio("Morning_Call", buffer)

def relatorio_clientes_cronicos():
    print(f"\n{Cores.VERMELHO}🚑 CLIENTES CRÔNICOS (REINCIDENTES){Cores.RESET}")
    print("Clientes afetados por múltiplas falhas/retrabalhos recentemente.")
    
    buffer = []
    buffer.append(f"🚑 *CLIENTES CRÔNICOS - {REGIONAL_ATUAL_NOME}*")
    query = """
        SELECT login, nome, endereco, COUNT(*) as qtd
        FROM clientes_afetados
        GROUP BY login
        HAVING qtd >= 3
        ORDER BY qtd DESC
        LIMIT 10
    """
    bd.cursor.execute(query)
    rows = bd.cursor.fetchall()
    
    if rows:
        buffer.append("\n📉 *Afetados por Quedas Recorrentes (+3x):*")
        for r in rows:
            buffer.append(f"   • {r[0]} ({r[1]}): Caiu {r[3]} vezes")
            buffer.append(f"     End: {r[2]}")
    
    query_rework = """
        SELECT cliente, COUNT(*) as qtd
        FROM retrabalhos
        GROUP BY cliente
        HAVING qtd >= 2
        ORDER BY qtd DESC
        LIMIT 10
    """
    bd.cursor.execute(query_rework)
    rows_rw = bd.cursor.fetchall()
    
    if rows_rw:
        buffer.append("\n🛠️ *Retrabalho Recorrente (+2 OS):*")
        for r in rows_rw:
            buffer.append(f"   • {r[0]}: {r[1]} chamados")

    if not rows and not rows_rw:
        buffer.append("✅ Nenhum cliente com alta reincidência detectado no histórico.")
        
    imprimir_e_salvar("Clientes_Cronicos", buffer)

def atualizar_estrutura_config():
    if not NOME_ARQUIVO_CONFIG.exists():
        return
    try:
        with open(NOME_ARQUIVO_CONFIG, "r+", encoding="utf-8") as f:
            config_data = json.load(f)
            precisa_salvar = False
            if "regional_data" in config_data:
                for regional_info in config_data["regional_data"].values():
                    if "gestor_email" not in regional_info:
                        regional_info["gestor_email"] = ""
                        precisa_salvar = True
                    if "telegram_chat_id" not in regional_info:
                        regional_info["telegram_chat_id"] = ""
                        precisa_salvar = True
            if "email_sender_settings" not in config_data:
                config_data["email_sender_settings"] = {
                    "smtp_server": "smtp.gmail.com",
                    "smtp_port": 587,
                    "sender_email": "",
                    "sender_password": "",
                }
                precisa_salvar = True
            if "telegram_settings" not in config_data:
                config_data["telegram_settings"] = {"bot_token": ""}
                precisa_salvar = True
            if "map_outage_threshold" not in config_data:
                config_data["map_outage_threshold"] = 20
                precisa_salvar = True
                
            if precisa_salvar:
                f.seek(0)
                json.dump(config_data, f, indent=4, ensure_ascii=False)
                f.truncate()
                print(
                    f"{Cores.VERDE}Arquivo de configuração atualizado com os novos campos!{Cores.RESET}"
                )
                time.sleep(2)
    except Exception as e:
        print(
            f"{Cores.VERMELHO}[ERRO] Não foi possível ler/atualizar o config: {e}{Cores.RESET}"
        )
        input("Pressione Enter para sair.")
        exit()

def salvar_configuracao():
    try:
        dados_para_salvar = {
            "last_regional": REGIONAL_ATUAL_NOME,
            "regional_data": CONFIG_REGIONAIS,
            "column_names": CONFIG_COLUNAS,
            "os_type_groups": CONFIG_OS_TIPOS,
            "email_sender_settings": EMAIL_SENDER_SETTINGS,
            "telegram_settings": TELEGRAM_SETTINGS,
            "map_outage_threshold": MINIMO_PARA_ALERTA_QUEDA,
        }
        with open(NOME_ARQUIVO_CONFIG, "w", encoding="utf-8") as f:
            json.dump(dados_para_salvar, f, indent=4, ensure_ascii=False)
        return True
    except Exception as e:
        print(
            f"\n{Cores.VERMELHO}[ERRO CRÍTICO] Não foi possível escrever o config. Erro: {e}{Cores.RESET}"
        )
        return False


def carregar_configuracao():
    global CONFIG_REGIONAIS, REGIONAL_ATUAL_NOME, CONFIG_COLUNAS, CONFIG_OS_TIPOS, EMAIL_SENDER_SETTINGS, TELEGRAM_SETTINGS, INTERVALO_MONITORAMENTO_PADRAO, MINIMO_PARA_ALERTA_QUEDA
    CONFIG_REGIONAIS_PADRAO = {
        "Exemplo": {
            "valor_api": "Regional | Exemplo",
            "empresas_retrab": [],
            "cidades": [],
            "gestor_email": "",
            "telegram_chat_id": "",
        }
    }
    CONFIG_COLUNAS_PADRAO = {
        "NOME_COLUNA_OS": "Ordem de Serviço",
        "NOME_COLUNA_CLIENTE": "Codigo de Parceiro",
        "NOME_COLUNA_CIDADE": "Cidade",
        "NOME_COLUNA_STATUS_SLA": "Status SLA",
        "NOME_COLUNA_STATUS_OS": "Status",
        "NOME_COLUNA_TEMPO_ATRASO": "Tempo Restante",
        "NOME_COLUNA_TIPO_OS": "Tipo de OS",
    }
    CONFIG_OS_TIPOS_PADRAO = {
        "Suporte_e_Upgrade": ["SUPORTE", "UPGRADE"],
        "Instalacao_e_Mudanca": ["INSTALACAO", "MUDANCA DE ENDERECO"],
    }
    EMAIL_SENDER_SETTINGS_PADRAO = {
        "smtp_server": "smtp.gmail.com",
        "smtp_port": 587,
        "sender_email": "",
        "sender_password": "",
    }
    TELEGRAM_SETTINGS_PADRAO = {"bot_token": ""}
    
    if NOME_ARQUIVO_CONFIG.exists():
        try:
            with open(NOME_ARQUIVO_CONFIG, "r", encoding="utf-8") as f:
                config_salva = json.load(f)
            CONFIG_REGIONAIS = config_salva.get("regional_data", CONFIG_REGIONAIS_PADRAO)
            REGIONAL_ATUAL_NOME = config_salva.get("last_regional", list(CONFIG_REGIONAIS.keys())[0])
            CONFIG_COLUNAS = config_salva.get("column_names", CONFIG_COLUNAS_PADRAO)
            CONFIG_OS_TIPOS = config_salva.get("os_type_groups", CONFIG_OS_TIPOS_PADRAO)
            EMAIL_SENDER_SETTINGS = config_salva.get("email_sender_settings", EMAIL_SENDER_SETTINGS_PADRAO)
            TELEGRAM_SETTINGS = config_salva.get("telegram_settings", TELEGRAM_SETTINGS_PADRAO)
            MINIMO_PARA_ALERTA_QUEDA = config_salva.get("map_outage_threshold", 20)
        except Exception as e:
            print(f"{Cores.AMARELO}AVISO: Erro config: {e}{Cores.RESET}")
            CONFIG_REGIONAIS = CONFIG_REGIONAIS_PADRAO
    else:
        print(f"{Cores.CIANO}Criando arquivo de configuração padrão...{Cores.RESET}")
        CONFIG_REGIONAIS = CONFIG_REGIONAIS_PADRAO
        CONFIG_COLUNAS = CONFIG_COLUNAS_PADRAO
        CONFIG_OS_TIPOS = CONFIG_OS_TIPOS_PADRAO
        EMAIL_SENDER_SETTINGS = EMAIL_SENDER_SETTINGS_PADRAO
        TELEGRAM_SETTINGS = TELEGRAM_SETTINGS_PADRAO
        REGIONAL_ATUAL_NOME = list(CONFIG_REGIONAIS.keys())[0]
        if not salvar_configuracao():
            input("\nPressione Enter para fechar.")
            exit()
    atualizar_variaveis_globais()


def atualizar_variaveis_globais():
    global REGIONAL_ATUAL_NOME, REGIONAL_ATUAL_VALOR_API, FILTRO_EMPRESAS_RETRABALHO, CIDADES_DA_REGIONAL_ATUAL, config_atual
    if REGIONAL_ATUAL_NOME not in CONFIG_REGIONAIS:
        REGIONAL_ATUAL_NOME = (
            list(CONFIG_REGIONAIS.keys())[0] if CONFIG_REGIONAIS else ""
        )
    config_atual = CONFIG_REGIONAIS.get(REGIONAL_ATUAL_NOME, {})
    REGIONAL_ATUAL_VALOR_API = config_atual.get("valor_api", "")
    FILTRO_EMPRESAS_RETRABALHO = config_atual.get("empresas_retrab", [])
    CIDADES_DA_REGIONAL_ATUAL = config_atual.get("cidades", [])


def logar_alerta(mensagem):
    try:
        with open(NOME_ARQUIVO_LOG, "a", encoding="utf-8") as f:
            f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {mensagem}\n")
    except Exception as e:
        print(
            f"{Cores.VERMELHO}[ERRO] Não foi possível escrever no arquivo de log: {e}{Cores.RESET}"
        )

def imprimir_tabela_bonita(cabecalhos, dados, cor_borda=Cores.AMARELO, cor_texto=Cores.RESET):
    if not dados:
        print(f"{Cores.AMARELO}Nenhum dado para exibir.{Cores.RESET}")
        return


    larguras = [len(c) for c in cabecalhos]
    for linha in dados:
        for i, valor in enumerate(linha):

            val_str = str(valor)
            if len(val_str) > 40: val_str = val_str[:37] + "..."

            if i < len(larguras):
                larguras[i] = max(larguras[i], len(val_str))


    linha_divisoria = cor_borda + "+" + "+".join(["-" * (l + 2) for l in larguras]) + "+" + Cores.RESET


    print(linha_divisoria)
    header_str = cor_borda + "|" + Cores.RESET
    for i, cab in enumerate(cabecalhos):
        header_str += f" {Cores.CIANO}{cab.center(larguras[i])}{Cores.RESET} {cor_borda}|{Cores.RESET}"
    print(header_str)
    print(linha_divisoria)


    for linha in dados:
        row_str = cor_borda + "|" + cor_texto
        for i, valor in enumerate(linha):
            val_str = str(valor)
            if len(val_str) > 40: val_str = val_str[:37] + "..."
            if val_str.replace('.', '', 1).isdigit():
                row_str += f" {val_str.rjust(larguras[i])} "
            else:
                row_str += f" {val_str.ljust(larguras[i])} "
            
            row_str += f"{cor_borda}|{cor_texto}"
        print(row_str + Cores.RESET)
    
    print(linha_divisoria)
def construir_url_com_regional(base_url, id_filtro, regional_valor_api):
    if "?" not in base_url:
        return base_url
    base = base_url.split("?")[0]
    parametro = [
        {
            "type": "string/=",
            "value": [regional_valor_api],
            "id": id_filtro,
            "target": ["dimension", ["template-tag", "regional"]],
        }
    ]
    return f"{base}?parameters={urllib.parse.quote(json.dumps(parametro))}"


def buscar_dados(url):
    try:
        response = cffi_requests.get(
            url,
            headers=HEADERS,
            cookies=COOKIES,
            timeout=TIMEOUT_SEGUNDOS,
            impersonate="chrome110",
        )
        if response.status_code not in [200, 202]:
            print(
                f"\n{Cores.AMARELO}[ERRO] Falha ao buscar dados. Status: {response.status_code}{Cores.RESET}"
            )
            return None, None
        data = response.json().get("data", {})
        return data.get("rows", []), data.get("cols", [])
    except Exception as e:
        print(f"\n{Cores.AMARELO}[ERRO INESPERADO] {e}{Cores.RESET}")
        return None, None

def buscar_dados_mapa(payload):
    try:
        response = standard_requests.post(URL_API_MAPA, json=payload, headers=HEADERS_MAPA, timeout=60)
        if response.status_code == 200: return response.json()
        elif response.status_code == 401: print(f"{Cores.VERMELHO}⚠️ Token do Mapa Expirado!{Cores.RESET}")
        return None
    except Exception as e:
        print(f"Erro conexão mapa: {e}")
        return None

def extrair_localizacao_mapa(endereco):
    bairro = "INDEFINIDO"
    cidade = "INDEFINIDA"
    try:
        partes = endereco.split(',')
        if len(partes) >= 2:
            parte_cidade = partes[-1].split('|')[0].strip()
            parte_bairro = partes[-2].strip()
            if len(parte_cidade) > 2: cidade = parte_cidade.upper()
            if len(parte_bairro) > 2: bairro = parte_bairro.upper()
            for c in LISTA_METROPOLITANA_SUB_2:
                if c['nome'].upper() in bairro:
                    cidade = c['nome'].upper()
                    bairro = "GERAL"
    except: pass
    return bairro, cidade

def gerar_relatorio_queda(cidade, bairro, clientes_df):
    try:
        if not PASTA_QUEDAS.exists():
            PASTA_QUEDAS.mkdir()
        agora = datetime.now()
        timestamp = agora.strftime("%Y-%m-%d_%H-%M-%S")
        cidade_safe = remover_acentos(cidade).replace(" ", "_")
        bairro_safe = remover_acentos(bairro).replace(" ", "_").replace("/", "-")
        nome_arquivo = PASTA_QUEDAS / f"QUEDA_{cidade_safe}_{bairro_safe}_{timestamp}.txt"
        
        qtd = len(clientes_df)
        conteudo = []
        conteudo.append("="*60)
        conteudo.append(f"RELATÓRIO DE QUEDA MASSIVA - {agora.strftime('%d/%m/%Y %H:%M:%S')}")
        conteudo.append("="*60)
        conteudo.append(f"CIDADE: {cidade}")
        conteudo.append(f"BAIRRO/REGIÃO: {bairro}")
        conteudo.append(f"TOTAL DE CLIENTES AFETADOS: {qtd}")
        conteudo.append("-" * 60)
        conteudo.append("LISTA DE CLIENTES:")
        conteudo.append("-" * 60)
        
        for _, row in clientes_df.iterrows():
            nome = row.get('cliente', 'N/D')
            end = row.get('endereco', 'N/D')
            login = row.get('login', 'N/D')
            conteudo.append(f"Nome: {nome}")
            conteudo.append(f"Login: {login}")
            conteudo.append(f"Endereço: {end}")
            conteudo.append("." * 40)
            
        conteudo.append("="*60)
        conteudo.append("FIM DO RELATÓRIO")
        
        with open(nome_arquivo, "w", encoding="utf-8") as f:
            f.write("\n".join(conteudo))
        print(f"{Cores.VERDE}📄 Relatório de Queda gerado: {nome_arquivo.name}{Cores.RESET}")
    except Exception as e:
        print(f"{Cores.VERMELHO}[ERRO AO GERAR RELATÓRIO] {e}{Cores.RESET}")

def enviar_alerta_telegram(message, chat_id):
    bot_token = TELEGRAM_SETTINGS.get("bot_token")
    if not bot_token or not chat_id:
        return
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}
    try:
        cffi_requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"{Cores.VERMELHO}[ERRO TELEGRAM] {e}{Cores.RESET}")

def disparar_alerta(novas_rows, tipo_alerta, cols=None, mapa_info=None):
    try:
        tipo_alerta_key = tipo_alerta.replace(" ", "_").upper()
        if tipo_alerta_key == "RECUPERACAO_MASSIVA":
             arquivo_de_som = PASTA_DE_SONS / "sucesso.mp3" if (PASTA_DE_SONS / "sucesso.mp3").exists() else None
        else:
             arquivo_de_som = SONS_DE_ALERTA.get(tipo_alerta_key, SONS_DE_ALERTA.get("RETRABALHO"))
        
        if SOUND_ENABLED and arquivo_de_som and arquivo_de_som.exists():
            try:
                playsound(str(arquivo_de_som), block=False)
            except Exception:
                print("\a")
        else:
            print("\a")

        width_inner = 60
        width_border = width_inner

        if mapa_info:
            is_queda = tipo_alerta_key == "QUEDA_MASSIVA"
            cor_info = Cores.VERMELHO if is_queda else Cores.VERDE
            cor_borda = Cores.VERMELHO if is_queda else Cores.VERDE
            titulo_box = "QUEDA MASSIVA DETECTADA" if is_queda else "REGIÃO NORMALIZADA (VOLTOU)"
            simbolo = "🔥" if is_queda else "✅"
            msg_log = f"ALERTA MAPA: {mapa_info['titulo']}"
            logar_alerta(msg_log)

            print(f"\n{cor_borda}╔{'═'*width_border}╗{Cores.RESET}")
            texto_titulo = f"{simbolo} {titulo_box} {simbolo}"
            padding_titulo = width_inner - len_visivel(texto_titulo)
            pad_l = padding_titulo // 2
            pad_r = padding_titulo - pad_l
            print(f"{cor_borda}║{Cores.RESET}{' '*pad_l}{cor_info}{titulo_box}{Cores.RESET}{' '*pad_r}{cor_borda}║{Cores.RESET}")
            print(f"{cor_borda}╠{'═'*width_border}╣{Cores.RESET}")
            
            def print_row(lbl, val):
                sys.stdout.write(f"{cor_borda}║{Cores.RESET} {lbl} {cor_info}{val}{Cores.RESET}")
                visivel = len(f" {lbl} {val}")
                restante = width_inner - visivel
                print(f"{' '*restante}{cor_borda}║{Cores.RESET}")

            print_row("Cidade:", mapa_info['cidade'])
            print_row("Bairro:", mapa_info['bairro'])
            if is_queda:
                print_row("Qtd Offline:", str(mapa_info['qtd']))
                print_row("Link:", mapa_info['link'])
            else:
                print_row("Status:", "Região Operando Normalmente")
            print(f"{cor_borda}╚{'═'*width_border}╝{Cores.RESET}")

        else:
            titulo_alerta = (
                "O.S. AGENDAMENTO (ALERTA DE VOLUME)"
                if tipo_alerta_key == "OS_AGENDAMENTO_MASSA"
                else tipo_alerta.replace("_", " ")
            )
            logar_alerta(f"ALERTA '{titulo_alerta}': {len(novas_rows)} novo(s) item(ns).")
            
            print(f"\n{Cores.VERMELHO}╔{'!'*width_border}╗{Cores.RESET}")
            msg_alerta = "!!! ALERTA: NOVOS ITENS ENCONTRADOS !!!"
            pad = width_inner - len(msg_alerta)
            print(f"{Cores.VERMELHO}║{Cores.RESET}{' '*(pad//2)}{msg_alerta}{' '*(pad - pad//2)}{Cores.VERMELHO}║{Cores.RESET}")
            pad2 = width_inner - len(titulo_alerta)
            print(f"{Cores.VERMELHO}║{Cores.RESET}{' '*(pad2//2)}{titulo_alerta}{' '*(pad2 - pad2//2)}{Cores.VERMELHO}║{Cores.RESET}")
            print(f"{Cores.VERMELHO}╚{'!'*width_border}╝{Cores.RESET}")

            if tipo_alerta_key == "RETRABALHO":
                exibir_tabela_reincidencias(novas_rows, REGIONAL_ATUAL_NOME, titulo_extra="(NOVAS)")
            elif tipo_alerta_key.startswith("OS_AGENDAMENTO") and cols:
                exibir_tabela_os(novas_rows, cols, REGIONAL_ATUAL_NOME, titulo_extra="(NOVAS)")

        chat_id = config_atual.get("telegram_chat_id")
        if chat_id:
            if mapa_info:
                if tipo_alerta_key == "QUEDA_MASSIVA":
                    msg_tg = (f"🚨 *ALERTA DE QUEDA MASSIVA* 🚨\nCidade: *{mapa_info['cidade']}*\nBairro: *{mapa_info['bairro']}*\nClientes Offline: *{mapa_info['qtd']}*\n[Ver no Mapa]({mapa_info['link']})")
                else: 
                    msg_tg = (f"✅ *RETORNO DE SINAL (NORMALIZADO)* ✅\nCidade: *{mapa_info['cidade']}*\nBairro: *{mapa_info['bairro']}*\nA região parece ter estabilizado.")
                enviar_alerta_telegram(msg_tg, chat_id)
            else:
                mensagem_telegram = formatar_mensagem_telegram(novas_rows, tipo_alerta_key, cols)
                enviar_alerta_telegram(mensagem_telegram, chat_id)
    except Exception as e:
        print(f"{Cores.VERMELHO}[ERRO CRÍTICO] {e}{Cores.RESET}")

def formatar_mensagem_telegram(novas_rows, tipo_alerta, cols=None):
    if tipo_alerta == "RETRABALHO":
        titulo = f"🚨 *ALERTA DE RETRABALHO* 🚨\nRegional: {REGIONAL_ATUAL_NOME}\n"
        linhas = [f"• *OS:* {row[0]} - {row[5]}" for row in novas_rows]
        return titulo + "\n".join(linhas)
    elif tipo_alerta.startswith("OS_AGENDAMENTO"):
        titulo = f"📢 *NOVAS O.S. PARA AGENDAMENTO* 📢\nRegional: {REGIONAL_ATUAL_NOME}\n"
        try:
            col_names = [c["name"] for c in cols]
            idx_os_id = col_names.index(CONFIG_COLUNAS["NOME_COLUNA_OS"])
            idx_cliente = col_names.index(CONFIG_COLUNAS["NOME_COLUNA_CLIENTE"])
            idx_cidade = col_names.index(CONFIG_COLUNAS["NOME_COLUNA_CIDADE"])
            linhas = [f"• *OS:* {row[idx_os_id]} - Cliente {row[idx_cliente]} em *{row[idx_cidade]}*" for row in novas_rows]
            return titulo + "\n".join(linhas)
        except Exception:
            return f"{titulo}Não foi possível formatar."
    return "Alerta recebido."

def monitorar_tudo():
    global historico_quedas_mapa
    print(f"\n{Cores.AMARELO}Iniciando Monitoramento UNIFICADO para '{REGIONAL_ATUAL_NOME}'...{Cores.RESET}")
    print(f"Incluindo monitoramento de Quedas Massivas.")
    print(f"⚠️ Mínimo para alerta de queda: {Cores.VERMELHO}{MINIMO_PARA_ALERTA_QUEDA}{Cores.RESET} clientes por Bairro.")
    
    rework_vistos_ids, os_agendamento_vistos_ids = set(), set()
    primeira_busca_rework, primeira_busca_os = True, True
    
    url_os_suporte = construir_url_com_regional(BASE_URL_OS_SUPORTE, ID_FILTRO_REGIONAL, REGIONAL_ATUAL_VALOR_API)
    url_os_mudanca = construir_url_com_regional(BASE_URL_OS_MUDANCA, ID_FILTRO_REGIONAL, REGIONAL_ATUAL_VALOR_API)
    urls_os_dinamicas = {"suporte_instalacao": url_os_suporte, "mudanca_upgrade": url_os_mudanca}
    cidades_lower = [c.lower() for c in CIDADES_DA_REGIONAL_ATUAL]
    
    payload_mapa = {"status": "todos", "caixa": [], "cidade": LISTA_METROPOLITANA_SUB_2, "bairro": [], "servicos_status": [{"id_servico_status": 11, "descricao": "Serviço Habilitado"}], "sem_cache": False}

    while True:
        try:
            timestamp = datetime.now().strftime("%H:%M:%S")
            print(f"\n{Cores.AMARELO}--- Ciclo de Verificação às {timestamp} ---{Cores.RESET}")
            
            # --- MAPA ---
            sys.stdout.write(f"\r🔍 Consultando Mapa... ")
            sys.stdout.flush()
            dados_mapa = buscar_dados_mapa(payload_mapa)
            if dados_mapa:
                lista_m = dados_mapa.get('marcadores', [])
                df_mapa = pd.DataFrame(lista_m)
                if 'status' in df_mapa.columns:
                    offline = df_mapa[df_mapa['status'] == 'offline'].copy()
                    total_off = len(offline)
                    total_on = len(df_mapa) - total_off
                    print(f"\r🗺️  Metropolitana SUB 2: {Cores.VERDE}ON: {total_on}{Cores.RESET} | {Cores.VERMELHO}OFF: {total_off}{Cores.RESET}            ")

                    contagem_localidades = {}
                    for _, row in offline.iterrows():
                        endereco = row.get('endereco', '')
                        bairro, cidade = extrair_localizacao_mapa(endereco)
                        chave = f"{cidade}|{bairro}"
                        if chave not in contagem_localidades:
                            contagem_localidades[chave] = {'qtd': 0, 'cidade': cidade, 'bairro': bairro, 'lat': row.get('latitude'), 'lng': row.get('longitude'), 'clientes': []}
                        contagem_localidades[chave]['qtd'] += 1
                        contagem_localidades[chave]['clientes'].append(row)

                    regioes_criticas_atuais = {}
                    for chave, dados in contagem_localidades.items():
                        if dados['qtd'] >= MINIMO_PARA_ALERTA_QUEDA:
                            regioes_criticas_atuais[chave] = dados


                    for chave, dados in regioes_criticas_atuais.items():
                        qtd_atual = dados['qtd']
                        cidade_atual = dados['cidade']
                        bairro_atual = dados['bairro']
                        

                        if bd.verificar_duplicidade_queda(cidade_atual, bairro_atual):
                            historico_quedas_mapa[chave] = dados
                            continue


                        qtd_historico = historico_quedas_mapa.get(chave, {}).get('qtd', 0)
                        

                        if chave not in historico_quedas_mapa or qtd_atual > (qtd_historico + 10):
                            df_afetados = pd.DataFrame(dados['clientes'])
                            gerar_relatorio_queda(cidade_atual, bairro_atual, df_afetados)
                            
                            # Registra no BD
                            bd.registrar_queda(cidade_atual, bairro_atual, qtd_atual, df_afetados)
                            
                            mapa_info = {
                                "titulo": f"QUEDA MASSIVA: {cidade_atual} - {bairro_atual}", 
                                "cidade": cidade_atual, 
                                "bairro": bairro_atual, 
                                "qtd": qtd_atual, 
                                "link": f"https://www.google.com/maps?q={dados['lat']},{dados['lng']}"
                            }
                            disparar_alerta([], "QUEDA_MASSIVA", mapa_info=mapa_info)

                    historico_quedas_mapa = regioes_criticas_atuais
                    


                    for chave_hist, dados_hist in historico_quedas_mapa.items():

                        if chave_hist not in regioes_criticas_atuais:
                            cidade_voltou = dados_hist['cidade']
                            bairro_voltou = dados_hist['bairro']

                            bd.atualizar_status_queda(cidade_voltou, bairro_voltou, 'NORMALIZADO')
                            mapa_info_voltou = {
                                "titulo": f"NORMALIZADO: {cidade_voltou} - {bairro_voltou}", 
                                "cidade": cidade_voltou, 
                                "bairro": bairro_voltou, 
                                "qtd": 0, 
                                "link": "N/A"
                            }
                            disparar_alerta([], "RECUPERACAO_MASSIVA", mapa_info=mapa_info_voltou)


                    historico_quedas_mapa = regioes_criticas_atuais
            else:
                 print(f"\r🗺️  Metropolitana SUB 2: Erro API            ")
            

            sys.stdout.write(f"🔍 Verificando Retrabalhos ({REGIONAL_ATUAL_NOME})... ")
            sys.stdout.flush()
            
            rows_rework, cols_rework = buscar_dados(API_URL_RETRABALHO)
            
            if rows_rework:

                idx_cidade = -1
                if cols_rework:
                    for i, col in enumerate(cols_rework):
                        nome = col.get('name', '').lower()
                        display = col.get('display_name', '').lower()
                        if 'cidade' in nome or 'municipio' in nome or 'cidade' in display:
                            idx_cidade = i
                            break
                
                cidades_permitidas = [str(c).upper().strip() for c in CIDADES_DA_REGIONAL_ATUAL]
                
                rows_filtradas_regional = []
                
                if idx_cidade != -1 and cidades_permitidas:
                    for r in rows_rework:
                        if len(r) > idx_cidade:
                            cidade_row = str(r[idx_cidade]).upper().strip()

                            if cidade_row in cidades_permitidas:
                                rows_filtradas_regional.append(r)
                else:

                    rows_filtradas_regional = rows_rework 


                for row in rows_filtradas_regional: 
                    bd.registrar_retrabalho(row, cols_rework)

                if FILTRO_EMPRESAS_RETRABALHO:
                    rows_para_alerta = [
                        r for r in rows_filtradas_regional
                        if any(e.lower() in str(r[-1]).lower() for e in FILTRO_EMPRESAS_RETRABALHO)
                    ]
                    
                    ids_atuais_rework = {r[0] for r in rows_para_alerta}
                    
                    if primeira_busca_rework:
                        rework_vistos_ids = ids_atuais_rework
                        sys.stdout.write(f"{Cores.VERDE}OK ({len(rework_vistos_ids)} na regional){Cores.RESET}\n")
                        primeira_busca_rework = False
                    else:
                        novas_ids = ids_atuais_rework - rework_vistos_ids
                        if novas_ids:
                            novas_rows = [r for r in rows_para_alerta if r[0] in novas_ids]
                            sys.stdout.write(f"{Cores.VERMELHO}NOVOS NA REGIONAL!{Cores.RESET}\n")
                            disparar_alerta(novas_rows, "RETRABALHO")
                            rework_vistos_ids.update(novas_ids)
                        else:
                            sys.stdout.write(f"Nada novo.\n")
                else:
                    sys.stdout.write(f"{Cores.VERDE}Atualizado ({len(rows_filtradas_regional)} registros).{Cores.RESET}\n")
            else:
                sys.stdout.write(f"{Cores.AMARELO}Sem dados.{Cores.RESET}\n")
            
            # --- AGENDAMENTO ---
            sys.stdout.write(f"🔍 Verificando Agendamento... ")
            sys.stdout.flush()
            os_encontradas_bruto, colunas_modelo_os = [], []
            for url in urls_os_dinamicas.values():
                rows_os, cols_os = buscar_dados(url)
                if rows_os:
                    if not colunas_modelo_os: colunas_modelo_os = cols_os
                    os_encontradas_bruto.extend(rows_os)
            
            if colunas_modelo_os:
                col_names = [c["name"] for c in colunas_modelo_os]
                try:
                    idx_status = col_names.index(CONFIG_COLUNAS["NOME_COLUNA_STATUS_OS"])
                    idx_cidade = col_names.index(CONFIG_COLUNAS["NOME_COLUNA_CIDADE"])
                    idx_os_id = col_names.index(CONFIG_COLUNAS["NOME_COLUNA_OS"])
                    os_por_status = [row for row in os_encontradas_bruto if len(row) > idx_status and (row[idx_status] or "").lower() == STATUS_ALVO_OS.lower()]
                    os_encontradas = ([row for row in os_por_status if CIDADES_DA_REGIONAL_ATUAL and len(row) > idx_cidade and (row[idx_cidade] or "").lower() in cidades_lower] if CIDADES_DA_REGIONAL_ATUAL else os_por_status)
                    ids_atuais_os = {row[idx_os_id] for row in os_encontradas}
                    
                    if primeira_busca_os:
                        os_agendamento_vistos_ids = ids_atuais_os
                        sys.stdout.write(f"{Cores.VERDE}OK ({len(os_agendamento_vistos_ids)} carregados){Cores.RESET}\n")
                        primeira_busca_os = False
                    else:
                        novas_ids = ids_atuais_os - os_agendamento_vistos_ids
                        if novas_ids:
                            sys.stdout.write(f"{Cores.VERMELHO}NOVOS!{Cores.RESET}\n")
                            tipo_alerta = "OS_AGENDAMENTO_MASSA" if len(novas_ids) > 4 else "OS_AGENDAMENTO"
                            novas_rows = [row for row in os_encontradas if row[idx_os_id] in novas_ids]
                            # SQLITE REGISTRO
                            for row in novas_rows: bd.registrar_reagendamento(row, colunas_modelo_os)
                            disparar_alerta(novas_rows, tipo_alerta, colunas_modelo_os)
                            os_agendamento_vistos_ids.update(novas_ids)
                        else:
                            sys.stdout.write(f"Nada novo.\n")
                except ValueError:
                    sys.stdout.write(f"{Cores.VERMELHO}Erro colunas.{Cores.RESET}\n")
            else:
                sys.stdout.write("Sem dados.\n")

            time.sleep(INTERVALO_MONITORAMENTO_PADRAO)
        except KeyboardInterrupt:
            print(f"\n{Cores.AMARELO}Monitoramento interrompido.{Cores.RESET}")
            break
        except Exception as e:
            print(f"{Cores.VERMELHO}Erro loop: {e}{Cores.RESET}")
            time.sleep(INTERVALO_MONITORAMENTO_PADRAO)


def buscar_cliente_mapa():
    print(f"\n{Cores.CIANO}--- PAINEL DE CLIENTES (Metropolitana SUB 2) ---{Cores.RESET}")
    print("Carregando dados em tempo real...")
    payload = {"status": "todos", "caixa": [], "cidade": LISTA_METROPOLITANA_SUB_2, "bairro": [], "servicos_status": [{"id_servico_status": 11, "descricao": "Serviço Habilitado"}], "sem_cache": False}
    dados = buscar_dados_mapa(payload)
    if not dados: return
    lista = dados.get('marcadores', [])
    df = pd.DataFrame(lista)
    stats = {c['nome'].upper(): {'on': 0, 'off': 0} for c in LISTA_METROPOLITANA_SUB_2}
    for _, row in df.iterrows():
        status = 'off' if str(row.get('status')) == 'offline' else 'on'
        endereco = str(row.get('endereco', '')).upper()
        for cidade_nome in stats.keys():
            if cidade_nome in remover_acentos(endereco):
                stats[cidade_nome][status] += 1
                break
    
    width_inner = 66
    width_border = width_inner
    print(f"\n{Cores.AMARELO}╔{'═'*width_border}╗")
    print(f"║ {'RESUMO DE CONEXÕES POR CIDADE':^{width_inner}} ║")
    print(f"╠{'═'*width_border}╣{Cores.RESET}")
    total_on_geral = 0
    total_off_geral = 0
    for cidade, qtd in stats.items():
        on = qtd['on']
        off = qtd['off']
        total_on_geral += on
        total_off_geral += off
        lbl_cidade = f" {cidade:<22} :"
        lbl_on = " ON "
        val_on = f"{on:<5}"
        sep = " | "
        lbl_off = "OFF "
        val_off = f"{off:<5} "
        visivel = len(lbl_cidade + lbl_on + val_on + sep + lbl_off + val_off)
        padding = width_inner - visivel
        print(f"{Cores.AMARELO}║{Cores.RESET}{lbl_cidade}{Cores.VERDE}{lbl_on}{val_on}{Cores.RESET}{sep}{Cores.VERMELHO}{lbl_off}{val_off}{Cores.RESET}{' '*padding}{Cores.AMARELO}║{Cores.RESET}")
    print(f"{Cores.AMARELO}╠{'═'*width_border}╣")
    lbl_total = " TOTAL GERAL             :"
    val_on_t = f"{total_on_geral:<5}"
    val_off_t = f"{total_off_geral:<5} "
    visivel_total = len(lbl_total + lbl_on + val_on_t + sep + lbl_off + val_off_t)
    pad_total = width_inner - visivel_total
    print(f"║{lbl_total}{Cores.VERDE}{lbl_on}{val_on_t}{Cores.RESET}{sep}{Cores.VERMELHO}{lbl_off}{val_off_t}{Cores.RESET}{' '*pad_total}║")
    print(f"╚{'═'*width_border}╝{Cores.RESET}\n")

    while True:
        termo = input("🔍 Digite Nome/Login/Endereço para buscar (ou Enter para sair): ").strip().upper()
        if not termo: break
        encontrados = []
        for item in lista:
            texto_busca = f"{item.get('cliente', '')} {item.get('login', '')} {item.get('endereco', '')}".upper()
            if termo in texto_busca: encontrados.append(item)
        if encontrados:
            print(f"\n{Cores.VERDE}Encontrados {len(encontrados)} clientes:{Cores.RESET}")
            for cli in encontrados[:10]:
                status_cli = cli.get('status', 'desconhecido')
                cor = Cores.VERDE if status_cli == 'online' else Cores.VERMELHO
                print(f"• {cli.get('cliente')} | {cli.get('endereco')}\n  Status: {cor}{status_cli.upper()}{Cores.RESET} | Login: {cli.get('login')}\n  Mapa: https://www.google.com/maps?q={cli.get('latitude')},{cli.get('longitude')}\n{'-' * 40}")
            if len(encontrados) > 10: print(f"... e mais {len(encontrados)-10} resultados.")
        else: print(f"{Cores.AMARELO}Nenhum cliente encontrado.{Cores.RESET}")

def listar_os_agendamento():
    print(f"\n{Cores.ROXO}Listando O.S. '{STATUS_ALVO_OS}' para '{REGIONAL_ATUAL_NOME}'...{Cores.RESET}")
    url_os_suporte = construir_url_com_regional(BASE_URL_OS_SUPORTE, ID_FILTRO_REGIONAL, REGIONAL_ATUAL_VALOR_API)
    url_os_mudanca = construir_url_com_regional(BASE_URL_OS_MUDANCA, ID_FILTRO_REGIONAL, REGIONAL_ATUAL_VALOR_API)
    resultados_brutos, colunas_modelo = [], []
    for url in [url_os_suporte, url_os_mudanca]:
        rows, cols = buscar_dados(url)
        if rows:
            if not colunas_modelo: colunas_modelo = cols
            resultados_brutos.extend(rows)
    if not colunas_modelo: return
    try:
        col_names = [c["name"] for c in colunas_modelo]
        idx_status = col_names.index(CONFIG_COLUNAS["NOME_COLUNA_STATUS_OS"])
        idx_cidade = col_names.index(CONFIG_COLUNAS["NOME_COLUNA_CIDADE"])
        cidades_lower = [c.lower() for c in CIDADES_DA_REGIONAL_ATUAL]
        final_list = [r for r in resultados_brutos if len(r) > idx_status and (r[idx_status] or "").lower() == STATUS_ALVO_OS.lower() and (not CIDADES_DA_REGIONAL_ATUAL or (len(r) > idx_cidade and (r[idx_cidade] or "").lower() in cidades_lower))]
        exibir_tabela_os(final_list, colunas_modelo, REGIONAL_ATUAL_NOME, titulo_extra="(ATUAL)")
    except ValueError:
        diagnosticar_colunas(colunas_modelo)

def diagnosticar_colunas(cols):
    print(f"\n{Cores.AMARELO}--- DIAGNÓSTICO DE COLUNAS ---{Cores.RESET}")
    if not cols: return
    for col in cols: print(f"  - Display: '{col.get('display_name')}' | Name: '{col.get('name')}'")

def gerar_mensagem_whatsapp(os_vencidas, cols, cidade, nome_grupo):
    try:
        col_names = [c["name"] for c in cols]
        idx_os_id = col_names.index(CONFIG_COLUNAS["NOME_COLUNA_OS"])
        idx_parceiro = col_names.index(CONFIG_COLUNAS["NOME_COLUNA_CLIENTE"])
        idx_tempo_atraso = col_names.index(CONFIG_COLUNAS["NOME_COLUNA_TEMPO_ATRASO"])
        nome_grupo_formatado = nome_grupo.replace("_", " ")
        mensagem = f"Prezados,\n\nSegue a lista de Ordens de Serviço de *{nome_grupo_formatado}* com *SLA vencido* para a cidade de *{cidade}* que necessitam de atenção imediata:\n\n"
        for i, row in enumerate(os_vencidas, 1): mensagem += f"*{i}. O.S. {row[idx_os_id]}* - Cliente: {row[idx_parceiro]} | Atraso: *{row[idx_tempo_atraso]}*\n"
        mensagem += f"\nPor favor, verificar e dar andamento com urgência.\n\nAtenciosamente,\nEquipe de Operações"
        timestamp = datetime.now().strftime("%Y-%m-%d")
        cidade_arquivo = cidade.replace(" ", "_")
        nome_arquivo = PASTA_WHATSAPP / f"MENSAGEM_SLA_{nome_grupo}_{cidade_arquivo}_{timestamp}.txt"
        with open(nome_arquivo, "w", encoding="utf-8") as f: f.write(mensagem)
        print(f"\n{Cores.VERDE}Arquivo '{nome_arquivo.name}' gerado!{Cores.RESET}")
    except Exception: pass

def avaliar_sla_por_cidade(cidade_alvo):
    print(f"\n{Cores.CIANO}Buscando SLA Vencido para '{cidade_alvo}'...{Cores.RESET}")
    url_os_suporte = construir_url_com_regional(BASE_URL_OS_SUPORTE, ID_FILTRO_REGIONAL, REGIONAL_ATUAL_VALOR_API)
    url_os_mudanca = construir_url_com_regional(BASE_URL_OS_MUDANCA, ID_FILTRO_REGIONAL, REGIONAL_ATUAL_VALOR_API)
    resultados_brutos, colunas_modelo = [], []
    for url in [url_os_suporte, url_os_mudanca]:
        rows, cols = buscar_dados(url)
        if rows and cols:
            if not colunas_modelo: colunas_modelo = cols
            resultados_brutos.extend(rows)
    if not colunas_modelo: return
    try:
        col_names = [c["name"] for c in colunas_modelo]
        idx_cidade = col_names.index(CONFIG_COLUNAS["NOME_COLUNA_CIDADE"])
        idx_status_sla = col_names.index(CONFIG_COLUNAS["NOME_COLUNA_STATUS_SLA"])
        idx_tipo_os = col_names.index(CONFIG_COLUNAS["NOME_COLUNA_TIPO_OS"])
        os_vencidas_todas = [r for r in resultados_brutos if len(r) > max(idx_cidade, idx_status_sla, idx_tipo_os) and (r[idx_cidade] or "").strip().lower() == cidade_alvo.lower() and (r[idx_status_sla] or "").strip().lower() == STATUS_SLA_VENCIDO.lower()]
        exibir_tabela_sla(os_vencidas_todas, colunas_modelo, cidade_alvo)
        if os_vencidas_todas and input(f"\n{Cores.AMARELO}Gerar mensagem WhatsApp? (s/n): {Cores.RESET}").lower().strip() == "s":
            grupos_para_gerar = {}
            for os in os_vencidas_todas:
                tipo_os = (os[idx_tipo_os] or "").strip().upper()
                for nome_grupo, tipos_no_grupo in CONFIG_OS_TIPOS.items():
                    if tipo_os in [t.upper() for t in tipos_no_grupo]:
                        if nome_grupo not in grupos_para_gerar: grupos_para_gerar[nome_grupo] = []
                        grupos_para_gerar[nome_grupo].append(os)
                        break
            for nome_grupo, lista_os in grupos_para_gerar.items(): gerar_mensagem_whatsapp(lista_os, colunas_modelo, cidade_alvo, nome_grupo)
    except Exception: pass

def iniciar_menu_sla():
    if not CIDADES_DA_REGIONAL_ATUAL: return
    width_inner = 66
    width_border = width_inner
    while True:
        titulo = f"SLA VENCIDO - REGIONAL {REGIONAL_ATUAL_NOME.upper()}"
        if len(titulo) > width_inner - 4: titulo = titulo[:width_inner-7] + "..."
        print(f"\n{Cores.AMARELO}╔{'═'*width_border}╗")
        print(f"║ {titulo:^{width_inner}} ║")
        print(f"╠{'═'*width_border}╣")
        for i, cidade in enumerate(CIDADES_DA_REGIONAL_ATUAL, 1):
            line_content = f" [{str(i).zfill(2)}] {cidade}"
            padding = width_inner - len(line_content)
            print(f"║ {Cores.RESET}{line_content}{' '*padding}{Cores.AMARELO}║")
        print(f"╠{'═'*width_border}╣")
        print(f"║ {Cores.CIANO} [0]  Voltar ao Menu Principal{' '*(width_inner-30)}{Cores.AMARELO}║")
        print(f"╚{'═'*width_border}╝{Cores.RESET}")
        escolha = input("Selecione uma cidade: ").strip()
        if escolha == "0": break
        try:
            indice = int(escolha) - 1
            if 0 <= indice < len(CIDADES_DA_REGIONAL_ATUAL): avaliar_sla_por_cidade(CIDADES_DA_REGIONAL_ATUAL[indice])
        except Exception: pass
        input("\nEnter para continuar...")

def buscar_retrabalho_interativo():
    print(f"\n{Cores.CIANO}Buscando base completa e atualizando banco de dados...{Cores.RESET}")
    rows, cols = buscar_dados(API_URL_RETRABALHO) 
    
    if not rows: return


    sys.stdout.write(f"Processando {len(rows)} registros para histórico... ")
    for row in rows:
        bd.registrar_retrabalho(row, cols) 
    sys.stdout.write(f"{Cores.VERDE}Concluído!{Cores.RESET}\n")


    termo_busca = input("Digite o ID da O.S., Cliente ou Empresa: ").lower().strip()
    if not termo_busca: return
    resultados = [row for row in rows if termo_busca in str(row).lower()]
    if resultados: exibir_tabela_reincidencias(resultados, "Resultados da Busca", f"TERMO: '{termo_busca}'")
    else: print(f"\nNenhum resultado encontrado.")

def gerenciar_lista(tipo_lista, lista_atual):
    print(f"\n{Cores.CIANO}--- Gerenciando {tipo_lista} ---{Cores.RESET}")
    if not lista_atual:
        return [item.strip() for item in input(f"Insira a lista (separados por vírgula): ").split(",") if item.strip()]
    print(f"Atuais: {', '.join(lista_atual)}")
    acao = input(f"[1] Manter, [2] Adicionar mais, [3] Substituir? ").strip()
    if acao == "2":
        lista_atual.extend([item.strip() for item in input(f"Adicionar: ").split(",") if item.strip() and item.strip() not in lista_atual])
    elif acao == "3":
        return [item.strip() for item in input(f"Nova lista: ").split(",") if item.strip()]
    return list(dict.fromkeys(lista_atual))

def print_menu_line(cor_numero, numero, texto_antes_val, cor_val, valor, texto_depois_val, width_inner):
    visible_len = len(f"  [{numero}] {texto_antes_val}{valor}{texto_depois_val}")
    padding = width_inner - visible_len
    print(f"║  {cor_numero}[{numero}]{Cores.RESET} {texto_antes_val}{cor_val}{valor}{Cores.RESET}{texto_depois_val}{' '*padding}{Cores.AMARELO}║")

def iniciar_menu_config():
    global REGIONAL_ATUAL_NOME, INTERVALO_MONITORAMENTO_PADRAO, MINIMO_PARA_ALERTA_QUEDA
    width_inner = 72
    width_border = width_inner
    while True:
        os.system("cls" if os.name == "nt" else "clear")
        print(f"\n{Cores.AMARELO}╔{'═'*width_border}╗{Cores.RESET}")
        print(f"{Cores.AMARELO}║{'MENU DE CONFIGURAÇÕES':^{width_inner}}║{Cores.RESET}")
        print(f"{Cores.AMARELO}╠{'═'*width_border}╣{Cores.RESET}")
        print_menu_line(Cores.CIANO, "1", "Alterar Regional Ativa (Atual: ", Cores.VERDE, REGIONAL_ATUAL_NOME, ")", width_inner)
        print_menu_line(Cores.CIANO, "2", "Gerenciar Cidades da Regional", "", "", "", width_inner)
        print_menu_line(Cores.CIANO, "3", "Gerenciar Empresas de Retrabalho", "", "", "", width_inner)
        print_menu_line(Cores.CIANO, "4", "Gerenciar E-mail do Gestor da Regional", "", "", "", width_inner)
        print_menu_line(Cores.CIANO, "5", "Gerenciar Chat ID do Telegram da Regional", "", "", "", width_inner)
        print_menu_line(Cores.CIANO, "6", "Alterar Intervalo de Monitoramento (Atual: ", Cores.VERDE, f"{INTERVALO_MONITORAMENTO_PADRAO}s", ")", width_inner)
        print_menu_line(Cores.CIANO, "7", "Editar Configs de E-mail (Remetente)", "", "", "", width_inner)
        print_menu_line(Cores.CIANO, "8", "Editar Token do Bot do Telegram", "", "", "", width_inner)
        print_menu_line(Cores.CIANO, "9", "Alterar Mínimo Queda Massiva (Atual: ", Cores.VERDE, f"{MINIMO_PARA_ALERTA_QUEDA}", ")", width_inner)
        print_menu_line(Cores.VERMELHO, "10", "Zerar Banco de Dados (Apagar Tudo)", "", "", "", width_inner)
        
        print(f"{Cores.AMARELO}║{Cores.RESET}  [0] Salvar e Voltar ao Menu Principal{' '*(width_inner-39)}{Cores.AMARELO}║{Cores.RESET}")
        print(f"{Cores.AMARELO}╚{'═'*width_border}╝{Cores.RESET}")
        
        escolha = input("Opção: ").strip()
        if escolha == "1":
            nomes = list(CONFIG_REGIONAIS.keys())
            for i, n in enumerate(nomes, 1): print(f"[{i}] {n}")
            try:
                idx = int(input("Escolha: ")) - 1
                if 0 <= idx < len(nomes): REGIONAL_ATUAL_NOME = nomes[idx]; atualizar_variaveis_globais()
            except: pass
        elif escolha == "2": config_atual["cidades"] = gerenciar_lista("cidades", config_atual.get("cidades", []))
        elif escolha == "3": config_atual["empresas_retrab"] = gerenciar_lista("empresas", config_atual.get("empresas_retrab", []))
        elif escolha == "4": config_atual["gestor_email"] = input("Novo email: ").strip() or config_atual.get("gestor_email")
        elif escolha == "5": config_atual["telegram_chat_id"] = input("Novo Chat ID: ").strip() or config_atual.get("telegram_chat_id")
        elif escolha == "6": INTERVALO_MONITORAMENTO_PADRAO = int(input("Segundos: ") or INTERVALO_MONITORAMENTO_PADRAO)
        elif escolha == "7":
            EMAIL_SENDER_SETTINGS["sender_email"] = input("Email: ") or EMAIL_SENDER_SETTINGS.get("sender_email")
            EMAIL_SENDER_SETTINGS["sender_password"] = input("Senha App: ") or EMAIL_SENDER_SETTINGS.get("sender_password")
        elif escolha == "8": TELEGRAM_SETTINGS["bot_token"] = input("Token Bot: ") or TELEGRAM_SETTINGS.get("bot_token")
        elif escolha == "9": MINIMO_PARA_ALERTA_QUEDA = int(input("Minimo Queda: ") or MINIMO_PARA_ALERTA_QUEDA)
        elif escolha == "10": 
            confirmacao = input(f"\n{Cores.VERMELHO}TEM CERTEZA? ISSO APAGARÁ TODO O HISTÓRICO DE QUEDAS, RETRABALHO E OS! (s/n): {Cores.RESET}").lower()
            if confirmacao == "s":
                if bd.limpar_banco():
                    print(f"{Cores.VERDE}Banco de dados limpo com sucesso.{Cores.RESET}")
                else:
                    print(f"{Cores.VERMELHO}Erro ao limpar banco.{Cores.RESET}")
            else:
                print("Operação cancelada.")
        elif escolha == "0": salvar_configuracao(); break

def iniciar_menu_testes():
    width_inner = 66
    width_border = width_inner
    while True:
        print(f"\n{Cores.AMARELO}╔{'═'*width_border}╗{Cores.RESET}")
        print(f"{Cores.AMARELO}║{'MENU DE TESTES':^{width_inner}}║{Cores.RESET}")
        print(f"{Cores.AMARELO}╠{'═'*width_border}╣{Cores.RESET}")
        print_menu_line(Cores.RESET, "1", "Testar Alerta Sonoro de Retrabalho", "", "", "", width_inner)
        print_menu_line(Cores.RESET, "2", "Testar Alerta Sonoro de O.S. (Normal)", "", "", "", width_inner)
        print_menu_line(Cores.VERMELHO, "3", "Testar Alerta Sonoro de O.S. (Em Massa)", "", "", "", width_inner)
        print_menu_line(Cores.CIANO, "4", "Testar Envio de E-mail de Resumo", "", "", "", width_inner)
        print_menu_line(Cores.CIANO, "5", "Testar Envio de Mensagem Telegram", "", "", "", width_inner)
        print_menu_line(Cores.VERMELHO, "6", "Testar Alerta de Queda Massiva (Visual Novo)", "", "", "", width_inner)
        print_menu_line(Cores.VERDE, "7", "Testar Alerta de Retorno/Voltou (Visual Novo)", "", "", "", width_inner)
        print(f"{Cores.AMARELO}║{Cores.RESET}  [0] Voltar ao Menu Principal{' '*(width_inner-30)}{Cores.AMARELO}║{Cores.RESET}")
        print(f"{Cores.AMARELO}╚{'═'*width_border}╝{Cores.RESET}")
        
        op = input("Opção: ").strip()
        if op == "1": testar_alerta_rework()
        elif op == "2": testar_alerta_os()
        elif op == "3": testar_alerta_os_massa()
        elif op == "4": testar_envio_email()
        elif op == "5": testar_envio_telegram()
        elif op == "6": testar_alerta_mapa()
        elif op == "7": testar_alerta_recuperacao()
        elif op == "0": break
        input("Enter...")


def testar_alerta_rework(): 

    disparar_alerta(
        [[999999, datetime.now().isoformat(), "CLIENTE TESTE", "SUPORTE", "OS_ANT", "Cliente | Teste"]], 
        "RETRABALHO"
    )

def testar_alerta_os(): 

    disparar_alerta(
        [[100000, "CLIENTE TESTE", "CIDADE TESTE", STATUS_ALVO_OS]], 
        "OS_AGENDAMENTO", 
        [
            {"name":CONFIG_COLUNAS["NOME_COLUNA_OS"], "display_name": "OS"}, 
            {"name":CONFIG_COLUNAS["NOME_COLUNA_CLIENTE"], "display_name": "Cliente"}, 
            {"name":CONFIG_COLUNAS["NOME_COLUNA_CIDADE"], "display_name": "Cidade"},
            {"name":CONFIG_COLUNAS["NOME_COLUNA_STATUS_OS"], "display_name": "Status"}
        ]
    )

def testar_alerta_os_massa(): 

    disparar_alerta(
        [[i, f"CLI {i}", "CIDADE TESTE", STATUS_ALVO_OS] for i in range(100000, 100005)], 
        "OS_AGENDAMENTO_MASSA", 
        [
            {"name":CONFIG_COLUNAS["NOME_COLUNA_OS"], "display_name": "OS"}, 
            {"name":CONFIG_COLUNAS["NOME_COLUNA_CLIENTE"], "display_name": "Cliente"}, 
            {"name":CONFIG_COLUNAS["NOME_COLUNA_CIDADE"], "display_name": "Cidade"},
            {"name":CONFIG_COLUNAS["NOME_COLUNA_STATUS_OS"], "display_name": "Status"}
        ]
    )
def testar_alerta_mapa():
    df = pd.DataFrame([{"cliente": "JOAO", "endereco": "RUA X, CENTRO, BRUMADINHO", "login": "joao", "id_cliente": 1}])
    gerar_relatorio_queda("BRUMADINHO", "CENTRO", df)
    disparar_alerta([], "QUEDA_MASSIVA", mapa_info={"titulo": "SIMULACAO", "cidade": "BRUMADINHO", "bairro": "CENTRO", "qtd": 1, "link": "http://map"})
def testar_alerta_recuperacao(): disparar_alerta([], "RECUPERACAO_MASSIVA", mapa_info={"titulo": "NORMALIZADO", "cidade": "BRUMADINHO", "bairro": "CENTRO", "qtd": 0, "link": "N/A"})
def testar_envio_email(): enviar_email_resumo(config_atual.get("gestor_email"), REGIONAL_ATUAL_NOME, FILTRO_EMPRESAS_RETRABALHO, config_atual)
def testar_envio_telegram(): enviar_alerta_telegram("Teste Telegram", config_atual.get("telegram_chat_id"))

def buscar_retrabalhos_do_dia(filtro): return []
def buscar_os_vencidas_para_email(cfg): return [], None
def formatar_tabela_html(t, h, r): return f"<h3>{t}</h3>"
def enviar_email_resumo(d, n, f, c): print("Função de email chamada (Simulação/Real)")
def agendador_email_background():
    while True: time.sleep(60)

def exibir_tabela_reincidencias(rows, nome, titulo_extra=""):
    qtd = len(rows)
    print(f"\n{Cores.CIANO}╔{'═'*60}╗")
    print(f"║ RELATÓRIO DE RETRABALHO - {nome:^30} ║")
    print(f"╠{'═'*60}╣{Cores.RESET}")
    print(f"  {titulo_extra}")
    print(f"  📊 Total de Registros: {Cores.VERDE}{qtd}{Cores.RESET}")
    print(f"{Cores.CIANO}╚{'═'*60}╝{Cores.RESET}\n")

    if qtd == 0: return


    dados_formatados = []
    

    counts_empresa = {}

    for r in rows:
        id_os = r[0]
        data = r[1]
        tipo = r[3] if len(r) > 3 else "N/D"
        

        raw_cli = str(r[5]) if len(r) > 5 else "N/D"
        cliente = raw_cli.split('|')[0].strip()[:25] 
        empresa = raw_cli.split('|')[1].strip() if '|' in raw_cli else "N/D"
        

        counts_empresa[empresa] = counts_empresa.get(empresa, 0) + 1

        dados_formatados.append([id_os, data, cliente, tipo, empresa])


    if len(counts_empresa) > 1:
        top_empresas = sorted(counts_empresa.items(), key=lambda x: x[1], reverse=True)[:3]
        print(f"{Cores.AMARELO}Top Ofensores (Empresas):{Cores.RESET}")
        for emp, count in top_empresas:
            print(f"  • {emp}: {count}")
        print("")

    cabecalhos = ["OS", "Data Abertura", "Técnico", "Serviço/Tipo", "Empresa"]
    imprimir_tabela_bonita(cabecalhos, dados_formatados, cor_borda=Cores.VERMELHO)
def exibir_tabela_os(rows, cols, nome, titulo_extra=""):
    qtd = len(rows)
    print(f"\n{Cores.ROXO}╔{'═'*60}╗")
    print(f"║ MONITORAMENTO DE O.S. - {nome:^32} ║")
    print(f"╠{'═'*60}╣{Cores.RESET}")
    print(f"  {titulo_extra}")
    print(f"  📅 Pendentes de Agendamento: {Cores.AMARELO}{qtd}{Cores.RESET}")
    print(f"{Cores.ROXO}╚{'═'*60}╝{Cores.RESET}\n")

    if qtd == 0: return

    col_names = [c["name"] for c in cols]
    try:
        idx_os = col_names.index(CONFIG_COLUNAS["NOME_COLUNA_OS"])
        idx_cli = col_names.index(CONFIG_COLUNAS["NOME_COLUNA_CLIENTE"])
        idx_cid = col_names.index(CONFIG_COLUNAS["NOME_COLUNA_CIDADE"])
        idx_status = col_names.index(CONFIG_COLUNAS["NOME_COLUNA_STATUS_OS"])
    except:
        print(f"{Cores.VERMELHO}Erro mapeando colunas. Dados brutos:{Cores.RESET}")
        for r in rows: print(r)
        return

    dados_formatados = []
    for r in rows:
  
        if not isinstance(r, (list, tuple)): 
            continue 
       
        
        try:

            if len(r) > max(idx_os, idx_cli, idx_cid, idx_status):
                os_id = r[idx_os]
                cliente = str(r[idx_cli])[:30]
                cidade = r[idx_cid]
                status = r[idx_status]
                dados_formatados.append([os_id, cliente, cidade, status])
        except Exception:
            continue

    if dados_formatados:
        cabecalhos = ["OS ID", "Cliente", "Cidade", "Status Atual"]
        imprimir_tabela_bonita(cabecalhos, dados_formatados, cor_borda=Cores.ROXO, cor_texto=Cores.BRANCO)
def exibir_tabela_sla(rows, cols, cidade):
    qtd = len(rows)
    print(f"\n{Cores.VERMELHO}╔{'═'*60}╗")
    print(f"║ SLA VENCIDO / CRÍTICO - {cidade:^30} ║")
    print(f"╠{'═'*60}╣{Cores.RESET}")
    print(f"  ⚠️ Total Atrasado: {Cores.VERMELHO}{qtd}{Cores.RESET}")
    print(f"{Cores.VERMELHO}╚{'═'*60}╝{Cores.RESET}\n")

    if qtd == 0: 
        print(f"{Cores.VERDE}Nenhuma OS com SLA estourado nesta cidade!{Cores.RESET}")
        return

    col_names = [c["name"] for c in cols]
    try:
        idx_os = col_names.index(CONFIG_COLUNAS["NOME_COLUNA_OS"])
        idx_cli = col_names.index(CONFIG_COLUNAS["NOME_COLUNA_CLIENTE"])
        idx_atraso = col_names.index(CONFIG_COLUNAS["NOME_COLUNA_TEMPO_ATRASO"])
        idx_tipo = col_names.index(CONFIG_COLUNAS["NOME_COLUNA_TIPO_OS"])
    except:
        for r in rows: print(r)
        return

    dados_formatados = []
    for r in rows:
        dados_formatados.append([
            r[idx_os],
            str(r[idx_cli])[:25],
            r[idx_tipo],
            r[idx_atraso]
        ])

    cabecalhos = ["OS", "Cliente", "Tipo", "Tempo Atraso"]
    imprimir_tabela_bonita(cabecalhos, dados_formatados, cor_borda=Cores.VERMELHO)


def relatorio_massivas_em_aberto():
    buffer = []
    buffer.append(f"\n{Cores.VERMELHO}--- QUEDAS MASSIVAS ATIVAS AGORA ---{Cores.RESET}")
    buffer.append(f"Monitorando status: 'DETECTADO'")
    

    query = """
        SELECT data_registro, cidade, bairro, qtd_afetados 
        FROM quedas_massivas 
        WHERE status = 'DETECTADO'
        ORDER BY qtd_afetados DESC
    """
    
    try:
        bd.cursor.execute(query)
        rows = bd.cursor.fetchall()
        
        if not rows:
            print(f"\n{Cores.VERDE}Nenhuma queda massiva ativa no momento!{Cores.RESET}")
            print(f"(Tudo normalizado ou o monitoramento ainda não detectou novos eventos)")
            return

        buffer.append(f"{'HORA INÍCIO':<20} | {'CIDADE':<20} | {'BAIRRO':<25} | {'OFFLINE'}")
        buffer.append("-" * 75)
        
        total_afetados = 0
        for r in rows:
            data_fmt = str(r[0])[11:19] 
            cidade = r[1]
            bairro = r[2]
            qtd = r[3]
            total_afetados += qtd
            
            buffer.append(f"{data_fmt:<20} | {cidade:<20} | {bairro:<25} | {Cores.VERMELHO}{qtd}{Cores.RESET}")
        
        buffer.append("-" * 75)
        buffer.append(f"TOTAL DE CLIENTES AFETADOS AGORA: {Cores.VERMELHO}{total_afetados}{Cores.RESET}")
        
        imprimir_e_salvar("Massivas_Ativas", buffer)
        
    except Exception as e:
        print(f"Erro SQL Massivas Ativas: {e}")

def exportar_relatorio(titulo_base, linhas_conteudo):
    """
    Salva o conteúdo em um arquivo TXT na pasta INFO, removendo códigos de cor.
    Adiciona cabeçalho e formatação básica para WhatsApp.
    """
    if not PASTA_INFO.exists():
        PASTA_INFO.mkdir()
    
    agora_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    nome_arquivo = f"{titulo_base}_{agora_str}.txt"
    caminho_arquivo = PASTA_INFO / nome_arquivo
    
    conteudo_limpo = []
    conteudo_limpo.append(f"📅 *Relatório Gerado em:* {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    conteudo_limpo.append(f"📊 *{titulo_base.replace('_', ' ')}*")
    conteudo_limpo.append("") 
    
    for linha in linhas_conteudo:

        linha_limpa = remover_ansi(linha)

        if "|" in linha_limpa and "CIDADE" in linha_limpa:
             conteudo_limpo.append(f"*{linha_limpa}*")
        elif "---" in linha_limpa:
             conteudo_limpo.append("------------------------------------------------")
        else:
             conteudo_limpo.append(linha_limpa)
             
    try:
        with open(caminho_arquivo, "w", encoding="utf-8") as f:
            f.write("\n".join(conteudo_limpo))
        print(f"\n{Cores.VERDE}✅ Relatório salvo em: {caminho_arquivo.name}{Cores.RESET}")
    except Exception as e:
        print(f"\n{Cores.VERMELHO}Erro ao salvar arquivo: {e}{Cores.RESET}")

def imprimir_e_salvar(titulo_base, linhas_buffer):
    """Imprime na tela (com cores) e oferece salvar em TXT (sem cores)."""

    for linha in linhas_buffer:
        print(linha)
        

    resp = input(f"\n{Cores.AMARELO}Deseja salvar este relatório em TXT (Pasta info)? (s/n): {Cores.RESET}").lower().strip()
    if resp == 's':
        exportar_relatorio(titulo_base, linhas_buffer)

def gerar_relatorios_sql():
    while True:
        os.system("cls" if os.name == "nt" else "clear")
        width = 72
        print(f"\n{Cores.ROXO}╔{'═'*width}╗{Cores.RESET}")
        print(f"{Cores.ROXO}║{'RELATÓRIOS & INTELIGÊNCIA (BI)':^{width}}║{Cores.RESET}")
        print(f"{Cores.ROXO}╠{'═'*width}╣{Cores.RESET}")
        
        # --- PRIORIDADE ALTA ---
        print_menu_line(Cores.VERMELHO, "1", "VERIFICAR MASSIVAS ATIVAS AGORA", "", "", "", width)
        
        print(f"{Cores.ROXO}╠{'═'*width}╣{Cores.RESET}")
        # --- RANKINGS ---
        print_menu_line(Cores.CIANO, "2", "Ranking: Técnicos (Retrabalho)", "", "", "", width)
        print_menu_line(Cores.CIANO, "3", "Ranking: Empresas (Retrabalho)", "", "", "", width)
        print_menu_line(Cores.CIANO, "4", "Ranking: Bairros com mais Quedas", "", "", "", width)
        
        print(f"{Cores.ROXO}╠{'═'*width}╣{Cores.RESET}")
        # --- NOVO BLOCO: OPERACIONAL & PREVENTIVO ---
        print_menu_line(Cores.AMARELO, "5", "Risco de SLA (Próximas 4h)", "", "", "", width)
        print_menu_line(Cores.AMARELO, "6", "Clientes Crônicos (Reincidentes)", "", "", "", width)
        print_menu_line(Cores.AMARELO, "7", "Gerar Morning Call (Resumo Matinal)", "", "", "", width)

        print(f"{Cores.ROXO}╠{'═'*width}╣{Cores.RESET}")
        # --- HISTÓRICO ---
        print_menu_line(Cores.VERDE, "8", "Pesquisar Quedas por Data", "", "", "", width)
        print_menu_line(Cores.VERDE, "9", "Histórico Mensal de Quedas", "", "", "", width)
        print_menu_line(Cores.VERDE, "10", "Raio-X da Cidade", "", "", "", width)
        
        print(f"{Cores.ROXO}║{Cores.RESET}  [0] Voltar{' '*(width-12)}{Cores.ROXO}║{Cores.RESET}")
        print(f"{Cores.ROXO}╚{'═'*width}╝{Cores.RESET}")
        
        op = input("Opção: ").strip()
        
        if op == "0": break
        

        elif op == "1": relatorio_massivas_em_aberto()
        elif op == "2": relatorio_ranking_tecnicos()
        elif op == "3": relatorio_ranking_empresas()
        elif op == "4": relatorio_ranking_bairros()
        

        elif op == "5": relatorio_risco_sla()
        elif op == "6": relatorio_clientes_cronicos()
        elif op == "7": gerar_morning_call()
        

        elif op == "8": relatorio_quedas_por_data()
        elif op == "9": relatorio_historico_mensal()
        elif op == "10": relatorio_busca_cidade()
        
        else: print("Opção inválida.")
        input("\nEnter para continuar...")

def relatorio_quedas_periodo():
    buffer = []
    buffer.append(f"\n{Cores.CIANO}--- ESTATÍSTICAS DE QUEDAS ---{Cores.RESET}")
    queries = {
        "Hoje": "SELECT COUNT(*), SUM(qtd_afetados) FROM quedas_massivas WHERE date(data_registro) = date('now')",
        "Últimos 7 Dias": "SELECT COUNT(*), SUM(qtd_afetados) FROM quedas_massivas WHERE date(data_registro) >= date('now', '-7 days')",
        "Últimos 30 Dias": "SELECT COUNT(*), SUM(qtd_afetados) FROM quedas_massivas WHERE date(data_registro) >= date('now', '-30 days')"
    }
    buffer.append(f"{'PERÍODO':<20} | {'EVENTOS':<10} | {'CLIENTES AFETADOS':<15}")
    buffer.append("-" * 55)
    for p, q in queries.items():
        bd.cursor.execute(q)
        res = bd.cursor.fetchone()
        buffer.append(f"{p:<20} | {(res[0] or 0):<10} | {(res[1] or 0):<15}")
    
    imprimir_e_salvar("Estatisticas_Quedas", buffer)

def relatorio_ranking_bairros():
    buffer = []
    buffer.append(f"\n{Cores.CIANO}--- TOP 10 BAIRROS COM MAIS INSTABILIDADE ---{Cores.RESET}")
    bd.cursor.execute("SELECT cidade, bairro, COUNT(*) as q, SUM(qtd_afetados) FROM quedas_massivas GROUP BY cidade, bairro ORDER BY q DESC LIMIT 10")
    buffer.append(f"{'CIDADE':<20} | {'BAIRRO':<25} | {'QUEDAS':<10} | {'CLIENTES AFETADOS'}")
    buffer.append("-" * 65)
    for r in bd.cursor.fetchall(): buffer.append(f"{r[0]:<20} | {r[1]:<25} | {str(r[2]):<10} | {r[3]}")
    
    imprimir_e_salvar("Ranking_Bairros", buffer)

def relatorio_ranking_tecnicos():

    lista_empresas = FILTRO_EMPRESAS_RETRABALHO
    
    buffer = []
    buffer.append(f"\n{Cores.CIANO}--- RANKING TÉCNICOS ({REGIONAL_ATUAL_NOME}) ---{Cores.RESET}")
    buffer.append(f"Filtro: Empresas cadastradas no Menu 7")

    if not lista_empresas:
        print(f"\n{Cores.VERMELHO}[ERRO] Nenhuma empresa cadastrada no Menu 7!{Cores.RESET}")
        print("Vá em 'Configurações' -> 'Gerenciar Empresas de Retrabalho' e adicione os nomes.")
        print("Exemplo: 'Vero', 'Giganet', 'Valenet', etc.")
        return


    clausulas_like = []
    params = []
    for emp in lista_empresas:
        clausulas_like.append("empresa LIKE ?")
        params.append(f"%{emp}%")
    
    where_sql = " OR ".join(clausulas_like)
    
    query = f"""
        SELECT tecnico, COUNT(*) as qtd 
        FROM retrabalhos 
        WHERE tecnico IS NOT 'N/D' 
        AND tecnico IS NOT ''
        AND ({where_sql})
        GROUP BY tecnico 
        ORDER BY qtd DESC 
        LIMIT 15
    """
    
    try:
        bd.cursor.execute(query, params)
        rows = bd.cursor.fetchall()
        
        if not rows:
            print(f"\n{Cores.AMARELO}Nenhum retrabalho encontrado para as empresas: {lista_empresas}{Cores.RESET}")
            print("Dica: Verifique se os nomes no Menu 7 batem com os nomes salvos no Menu 6.")
            return

        buffer.append(f"{'POS':<4} | {'TÉCNICO':<35} | {'RETRABALHOS'}")
        buffer.append("-" * 60)
        
        for i, r in enumerate(rows, 1):
            nome_tec = r[0][:35]
            qtd = r[1]
            
            cor_pos = Cores.RESET
            if i == 1: cor_pos = Cores.VERMELHO
            elif i == 2: cor_pos = Cores.AMARELO
            elif i == 3: cor_pos = Cores.CIANO
            
            buffer.append(f"{cor_pos}{str(i)+'º':<4} | {nome_tec:<35} | {qtd}{Cores.RESET}")
        

        query_nd = f"SELECT COUNT(*) FROM retrabalhos WHERE (tecnico = 'N/D' OR tecnico = '') AND ({where_sql})"
        bd.cursor.execute(query_nd, params)
        sem_tec = bd.cursor.fetchone()[0]
        
        if sem_tec > 0:
            buffer.append("-" * 60)
            buffer.append(f"Registros nestas empresas sem técnico identificado: {sem_tec}")

        imprimir_e_salvar("Ranking_Tecnicos_Empresas", buffer)
        
    except Exception as e:
        print(f"Erro SQL Ranking Técnicos: {e}")

def relatorio_clientes_criticos():
    buffer = []
    buffer.append(f"\n{Cores.CIANO}--- TOP 10 CLIENTES CRÍTICOS ---{Cores.RESET}")
    bd.cursor.execute("SELECT nome, login, endereco, COUNT(*) as c FROM clientes_afetados GROUP BY login ORDER BY c DESC LIMIT 10")
    for r in bd.cursor.fetchall(): 
        buffer.append(f"• {r[0]} ({r[1]})")
        buffer.append(f"  Caiu {Cores.VERMELHO}{r[3]} vezes{Cores.RESET} | {r[2]}")
        buffer.append("-" * 40)
    
    imprimir_e_salvar("Clientes_Criticos", buffer)

def relatorio_ranking_empresas():
    lista_empresas = FILTRO_EMPRESAS_RETRABALHO
    
    buffer = []
    buffer.append(f"\n{Cores.CIANO}--- TOP EMPRESAS COM RETRABALHO ({REGIONAL_ATUAL_NOME}) ---{Cores.RESET}")
    
    if not lista_empresas:
        print(f"{Cores.VERMELHO}Erro: Lista de empresas vazia no Menu 7.{Cores.RESET}")
        return


    clausulas_like = []
    params = []
    for emp in lista_empresas:
        clausulas_like.append("empresa LIKE ?")
        params.append(f"%{emp}%")
    
    where_sql = " OR ".join(clausulas_like)
    
    query = f"""
        SELECT empresa, COUNT(*) as q 
        FROM retrabalhos 
        WHERE ({where_sql}) 
        GROUP BY empresa 
        ORDER BY q DESC 
        LIMIT 10
    """
    
    try:
        bd.cursor.execute(query, params)
        rows = bd.cursor.fetchall()
        
        if not rows:
            buffer.append("Nenhum registro encontrado para as empresas configuradas.")
        else:
            buffer.append(f"{'POS':<4} | {'EMPRESA':<40} | {'QTD'}")
            buffer.append("-" * 55)
            for i, r in enumerate(rows, 1):
                empresa = r[0] if r[0] != 'N/D' else 'Não Identificada'
                buffer.append(f"{str(i)+'º':<4} | {empresa:<40} | {r[1]}")
        
        imprimir_e_salvar("Ranking_Empresas_Regional", buffer)
    except Exception as e: print(e)

def relatorio_ranking_reagendamento():
    buffer = []
    buffer.append(f"\n{Cores.CIANO}--- CIDADES COM MAIS REAGENDAMENTOS ---{Cores.RESET}")
    bd.cursor.execute("SELECT cidade, COUNT(*) as q FROM reagendamentos GROUP BY cidade ORDER BY q DESC LIMIT 10")
    for r in bd.cursor.fetchall(): 
        buffer.append(f"{r[0]:<30} : {r[1]} reagendamentos")
    
    imprimir_e_salvar("Ranking_Reagendamentos", buffer)

def relatorio_quedas_por_data():
    buffer = []
    buffer.append(f"\n{Cores.CIANO}--- BUSCAR QUEDAS POR DATA ---{Cores.RESET}")

    data_input = input("Digite a data (DD/MM/AAAA): ").strip()
    try: data_fmt = datetime.strptime(data_input, "%d/%m/%Y").strftime("%Y-%m-%d")
    except: print("Data inválida."); return
    
    bd.cursor.execute("SELECT time(data_registro), cidade, bairro, qtd_afetados FROM quedas_massivas WHERE date(data_registro) = ? ORDER BY data_registro ASC", (data_fmt,))
    rows = bd.cursor.fetchall()
    
    if not rows: 
        print(f"{Cores.AMARELO}Nenhuma queda.{Cores.RESET}")
        return
        
    buffer.append(f"{'HORA':<10} | {'CIDADE':<20} | {'BAIRRO':<25} | {'CLIENTES'}")
    buffer.append("-" * 70)
    for r in rows: buffer.append(f"{r[0]:<10} | {r[1]:<20} | {r[2]:<25} | {r[3]}")
    
    imprimir_e_salvar(f"Quedas_Data_{data_fmt}", buffer)

def relatorio_historico_mensal():
    buffer = []
    buffer.append(f"\n{Cores.CIANO}--- HISTÓRICO MÊS A MÊS ---{Cores.RESET}")
    bd.cursor.execute("SELECT strftime('%m/%Y', data_registro), COUNT(*), SUM(qtd_afetados) FROM quedas_massivas GROUP BY strftime('%Y-%m', data_registro) ORDER BY data_registro DESC")
    rows = bd.cursor.fetchall()
    buffer.append(f"{'MÊS/ANO':<15} | {'EVENTOS':<15} | {'CLIENTES'}")
    buffer.append("-" * 50)
    for r in rows: buffer.append(f"{r[0]:<15} | {str(r[1]):<15} | {r[2]}")
    
    imprimir_e_salvar("Historico_Mensal", buffer)

def relatorio_busca_cidade():
    buffer = []
    buffer.append(f"\n{Cores.CIANO}--- RAIO-X POR CIDADE ---{Cores.RESET}")
    cidade = input("Cidade: ").strip()
    bd.cursor.execute("SELECT COUNT(*), SUM(qtd_afetados) FROM quedas_massivas WHERE cidade LIKE ?", (f"%{cidade}%",))
    geral = bd.cursor.fetchone()
    if not geral or geral[0] == 0: print("Sem registros."); return
    
    buffer.append(f"\n{Cores.VERDE}RESUMO: {cidade.upper()}{Cores.RESET}")
    buffer.append(f"Quedas: {geral[0]} | Clientes Afetados: {geral[1]}")
    buffer.append(f"{'-'*50}")
    
    bd.cursor.execute("SELECT bairro, COUNT(*), SUM(qtd_afetados) FROM quedas_massivas WHERE cidade LIKE ? GROUP BY bairro ORDER BY COUNT(*) DESC", (f"%{cidade}%",))
    buffer.append(f"{'BAIRRO':<30} | {'QUEDAS':<10} | {'CLIENTES AFETADOS'}")
    buffer.append("-" * 60)
    for r in bd.cursor.fetchall(): buffer.append(f"{r[0]:<30} | {str(r[1]):<10} | {r[2]}")
    
    imprimir_e_salvar(f"RaioX_{cidade}", buffer)

def exibir_cabecalho():
    print(
        f"{Cores.CIANO}==================================================================="
    )
    print(f"                  Monitor Unificado - Sempre Internet")
    print(
        f"                  (Regional Ativa: {Cores.VERDE}{REGIONAL_ATUAL_NOME}{Cores.CIANO})"
    )
    print(
        f"==================================================================={Cores.RESET}"
    )
    print("2.4 - Mapeamento & BI (TXT Export) - Desenvolvido por RRD\n")


def exibir_menu():
    width_inner = 72
    width_border = width_inner
    print(f"{Cores.AMARELO}╔{'═'*width_border}╗{Cores.RESET}")
    print(f"{Cores.AMARELO}║{'MONITORAMENTO':^{width_inner}}║{Cores.RESET}")
    print(f"{Cores.AMARELO}╠{'═'*width_border}╣{Cores.RESET}")
    
    print_menu_line(Cores.VERDE, "1", "Monitoramento", "", "", "", width_inner)
    
    print(f"{Cores.AMARELO}╠{'═'*width_border}╣{Cores.RESET}")
    print(f"{Cores.AMARELO}║{'CONSULTAS RÁPIDAS':^{width_inner}}║{Cores.RESET}")
    print(f"{Cores.AMARELO}╠{'═'*width_border}╣{Cores.RESET}")
    
    print_menu_line(Cores.CIANO, "2", "Listar Retrabalhos (Regional)", "", "", "", width_inner)
    print_menu_line(Cores.CIANO, "3", "Listar O.S. Agendamento", "", "", "", width_inner)
    print_menu_line(Cores.CIANO, "4", "Avaliar SLA (Gera Mensagem WhatsApp)", "", "", "", width_inner)
    print_menu_line(Cores.CIANO, "5", "Clientes (Busca e Resumo)", "", "", "", width_inner)
    print_menu_line(Cores.CIANO, "6", "Buscar Retrabalho Geral", "", "", "", width_inner)
    
    print(f"{Cores.AMARELO}╠{'═'*width_border}╣{Cores.RESET}")
    print(f"{Cores.AMARELO}║{'GERAIS':^{width_inner}}║{Cores.RESET}")
    print(f"{Cores.AMARELO}╠{'═'*width_border}╣{Cores.RESET}")
    
    print_menu_line(Cores.CIANO, "7", "Configurações (Regional/Emails)", "", "", "", width_inner)
    print_menu_line(Cores.CIANO, "8", "Testes do Sistema", "", "", "", width_inner)
    print_menu_line(Cores.CIANO, "9", "Sair", "", "", "", width_inner)
    
    print(f"{Cores.AMARELO}╠{'═'*width_border}╣{Cores.RESET}")
    print(f"{Cores.AMARELO}║{'INTELIGÊNCIA':^{width_inner}}║{Cores.RESET}")
    print(f"{Cores.AMARELO}╠{'═'*width_border}╣{Cores.RESET}")
    
    print_menu_line(Cores.ROXO, "10", "Relatórios & Histórico (BI)", "", "", "", width_inner)
    print_menu_line(Cores.CIANO, "11", "Previsão de Chuva (5 Dias)", "", "", "", width_inner)
    
    print(f"{Cores.AMARELO}╚{'═'*width_border}╝{Cores.RESET}")

def main():
    while True:
        os.system("cls" if os.name == "nt" else "clear")
        exibir_cabecalho()
        exibir_menu()
        comando = (
            input(f"{Cores.CIANO}Escolha uma opção: {Cores.RESET}").lower().strip()
        )
        if comando == "1":
            monitorar_tudo()
        elif comando == "2":
            print(f"\nBuscando retrabalhos para '{REGIONAL_ATUAL_NOME}'...")
            rows, cols = buscar_dados(API_URL_RETRABALHO) # Pega rows E cols
            if rows:

                for r in rows:
                    bd.registrar_retrabalho(r, cols)


                if FILTRO_EMPRESAS_RETRABALHO:
                    rows_filtradas = [
                        row
                        for row in rows
                        if any(
                            emp.lower() in str(row[-1]).lower()
                            for emp in FILTRO_EMPRESAS_RETRABALHO
                        )
                    ]
                    exibir_tabela_reincidencias(
                        rows_filtradas, REGIONAL_ATUAL_NOME, titulo_extra="(ATUAL)"
                    )
                else:
                    print(
                        f"{Cores.AMARELO}Nenhuma empresa cadastrada para esta regional. Impossível filtrar, mas os dados foram salvos no histórico.{Cores.RESET}"
                    )
            input("Pressione Enter para voltar...")
        elif comando == "3":
            listar_os_agendamento()
            input("Pressione Enter para voltar...")
        elif comando == "4":
            iniciar_menu_sla()
        elif comando == "5":
            buscar_cliente_mapa()
        elif comando == "6":
            buscar_retrabalho_interativo()
        elif comando == "11":
            menu_previsao_chuva()
            input("Pressione Enter para voltar...")
        elif comando == "7":
            iniciar_menu_config()
        elif comando == "8":
            iniciar_menu_testes()
        elif comando == "9":
            print("Salvando configurações antes de sair...")
            salvar_configuracao()
            print("Saindo...")
            time.sleep(1)
            break
        elif comando == "10":
            gerar_relatorios_sql()
        else:
            print(f"\n{Cores.VERMELHO}Opção inválida.{Cores.RESET}")
            time.sleep(1)


if __name__ == "__main__":
    if not PASTA_DE_SONS.exists():
        PASTA_DE_SONS.mkdir()
    if not PASTA_WHATSAPP.exists():
        PASTA_WHATSAPP.mkdir()
    if not PASTA_INFO.exists():
        PASTA_INFO.mkdir()
        
    atualizar_estrutura_config()
    carregar_configuracao()
    agendador_thread = threading.Thread(target=agendador_email_background, daemon=True)
    agendador_thread.start()
    main()