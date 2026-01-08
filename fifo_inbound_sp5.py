import asyncio
import os
import shutil
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import zipfile
from gspread_dataframe import set_with_dataframe  # Import para upload de DataFrame

# =================== CONFIGURAÇÕES ===================
DOWNLOAD_DIR = "/tmp/shopee_automation"
SPREADSHEET_ID = "1Ie3u58e-PT1ZEQJE20a6GJB-icJEXBRVDVxTzxCqq4c"
#JSON_KEYFILE = "C:/Users/SEAOps/Desktop/Eduardo/Não Mexer/FIFO INBOUND.json"  # coloque o caminho do JSON aqui
# PLANILHA_NOME = "FIFO INBOUND SP5"       # nome da planilha
ABA_NOME = "Base"                        # nome da aba fixa
# =====================================================

def unzip_and_process_data(zip_path, extract_to_dir):
    """
    Descompacta um ZIP, junta todos os CSVs e aplica o filtro de colunas.
    """
    try:
        unzip_folder = os.path.join(extract_to_dir, "extracted_files")
        os.makedirs(unzip_folder, exist_ok=True)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(unzip_folder)
        print(f"📂 Arquivo '{os.path.basename(zip_path)}' descompactado.")

        csv_files = [os.path.join(unzip_folder, f) for f in os.listdir(unzip_folder) if f.lower().endswith('.csv')]
        
        if not csv_files:
            print(f"⚠ Nenhum CSV encontrado no {zip_path}")
            shutil.rmtree(unzip_folder)
            return None

        print(f"📑 Lendo e unificando {len(csv_files)} arquivos CSV...")
        all_dfs = [pd.read_csv(file, encoding='utf-8') for file in csv_files]
        df_final = pd.concat(all_dfs, ignore_index=True)

        print("🔎 Aplicando filtros de colunas...")
        indices_para_manter = [0, 14, 39, 40, 48]
        df_final = df_final.iloc[:, indices_para_manter]

        shutil.rmtree(unzip_folder)  # limpa apenas a pasta de extração
        return df_final
    except Exception as e:
        print(f"❌ Erro processando {zip_path}: {e}")
        return None

def update_google_sheet_with_dataframe(df_to_upload):
    """Atualiza Google Sheets em uma aba fixa."""
    if df_to_upload is None or df_to_upload.empty:
        print(f"⚠ Nenhum dado para enviar para a aba '{ABA_NOME}'.")
        return
        
    try:
        print(f"⬆ Enviando dados para a aba '{ABA_NOME}'...")

        df_to_upload = df_to_upload.fillna("").astype(str)

        scope = [
            "https://spreadsheets.google.com/feeds",
            'https://www.googleapis.com/auth/spreadsheets',
            "https://www.googleapis.com/auth/drive"
        ]
        if not os.path.exists("hxh.json"):
            raise FileNotFoundError("O arquivo 'hxh.json' não foi encontrado.")

        creds = Credentials.from_service_account_file("hxh.json", scopes=scope)
        client = gspread.authorize(creds)
        
        planilha = client.open_by_key(SPREADSHEET_ID)

        # Cria ou pega a aba pelo nome definido
        try:
            aba = planilha.worksheet(ABA_NOME)
        except gspread.exceptions.WorksheetNotFound:
            aba = planilha.add_worksheet(title=ABA_NOME, rows="1000", cols="20")
        
        aba.clear()
        set_with_dataframe(aba, df_to_upload)
        
        print(f"✅ Dados enviados com sucesso para '{ABA_NOME}'!")
    except Exception as e:
        import traceback
        print(f"❌ Erro ao enviar para Google Sheets na aba '{ABA_NOME}':\n{traceback.format_exc()}")

async def main():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    try:
        zip_files = [os.path.join(DOWNLOAD_DIR, f) for f in os.listdir(DOWNLOAD_DIR) if f.lower().endswith(".zip")]
        
        if not zip_files:
            print("⚠ Nenhum arquivo .zip encontrado na pasta.")
            return

        print(f"🔍 Encontrados {len(zip_files)} arquivos ZIP.")

        dfs = []
        for zip_path in zip_files:
            df = unzip_and_process_data(zip_path, DOWNLOAD_DIR)
            if df is not None and not df.empty:
                dfs.append(df)

        if dfs:
            df_final = pd.concat(dfs, ignore_index=True)
            update_google_sheet_with_dataframe(df_final)
        else:
            print("⚠ Nenhum dado válido processado.")

    except Exception as e:
        print(f"❌ Erro no processo principal: {e}")

if __name__ == "__main__":
    asyncio.run(main())
