# -*- coding: utf-8 -*-
r"""
Script de Restauração do Backup Completo Particionado (Dados + Programas)
Execute no PC de Destino (Remoto)
Origem: N:\Meu Drive\PostgreSQL (ou onde estiverem as partes)
Destino: E:\ (recriará E:\PostgreSQL e E:\Programas automaticamente)
"""

import os
import sys
import time
import tarfile
import pathlib
import shutil
from tqdm import tqdm
from acs_toolbox.segredos import get_secret

def obter_origem_dir() -> pathlib.Path:
    env_origem = get_secret("BACKUP_ORIGEM_DIR", default=None)
    if env_origem:
        return pathlib.Path(env_origem)
    if len(sys.argv) > 1:
        return pathlib.Path(sys.argv[1])
    candidatos = [
        pathlib.Path(r"N:\Meu Drive\PostgreSQL"),
        pathlib.Path(r"J:\Meu Drive\PostgreSQL"),
        pathlib.Path(r"G:\Meu Drive\PostgreSQL"),
        pathlib.Path(r"D:\Meu Drive\PostgreSQL"),
    ]
    for c in candidatos:
        if c.exists():
            return c
    return candidatos[0]

ORIGEM_DIR = obter_origem_dir()
DESTINO_DIR = pathlib.Path("E:/")
NOME_BASE = "PostgreSQL_completo_backup"
ESPACO_MINIMO_LIVRE_GB = 195.0

class MultiVolumeReader:
    """Lê continuamente as partes sequenciais de 1GB como um único stream"""
    def __init__(self, lista_partes):
        self.lista_partes = lista_partes
        self.idx = 0
        self.current_file = None
        self._open_next()

    def _open_next(self):
        if self.current_file:
            self.current_file.close()
        if self.idx < len(self.lista_partes):
            self.current_file = open(self.lista_partes[self.idx], "rb")
            self.idx += 1
        else:
            self.current_file = None

    def read(self, size=-1):
        if not self.current_file:
            return b""
        data = self.current_file.read(size)
        if not data:
            self._open_next()
            if self.current_file:
                return self.read(size)
            return b""
        return data

    def close(self):
        if self.current_file:
            self.current_file.close()

def executar_restauracao():
    partes = sorted(list(ORIGEM_DIR.glob(f"{NOME_BASE}.part*.tar.gz")))
    if not partes:
        print(f"ERRO: Nenhuma parte encontrada em {ORIGEM_DIR} com o padrão {NOME_BASE}.part*.tar.gz")
        return

    DESTINO_DIR.mkdir(parents=True, exist_ok=True)
    tamanho_total_partes = sum(p.stat().st_size for p in partes)

    # Verificação de espaço em disco no destino
    try:
        uso_destino = shutil.disk_usage(DESTINO_DIR)
        livre_destino_gb = uso_destino.free / (1024**3)
        if livre_destino_gb < ESPACO_MINIMO_LIVRE_GB:
            print(f"\n[ALERTA CRÍTICO DE ESPAÇO] A unidade de destino '{DESTINO_DIR}' possui apenas {livre_destino_gb:.1f} GB livres.")
            print(f"O banco de dados descompactado requer aproximadamente {ESPACO_MINIMO_LIVRE_GB:.1f} GB.")
            print("Se a unidade 'E:' for um cartão MicroSD ou partição pequena, configure uma unidade virtual (ex: 'subst E: C:\\Disco_E').")
            resposta = input("Deseja continuar mesmo assim? (s/N): ").strip().lower()
            if resposta != 's':
                print("Operação cancelada pelo usuário.")
                return
    except Exception as e:
        print(f"Aviso: Não foi possível verificar o espaço livre em {DESTINO_DIR}: {e}")

    print("=" * 75)
    print("INICIANDO RESTAURAÇÃO COMPLETA (DADOS + PROGRAMAS) NO DESTINO")
    print(f"Origem das partes: {ORIGEM_DIR} ({len(partes)} partes, {tamanho_total_partes / (1024**3):.2f} GB comprimido)")
    print(f"Destino da extração: {DESTINO_DIR} (recriará E:\\PostgreSQL e E:\\Programas)")
    print("=" * 75)

    reader = MultiVolumeReader(partes)
    inicio = time.time()

    with tarfile.open(fileobj=reader, mode="r|gz") as tar:
        with tqdm(unit="arquivos", desc="Extraindo Dados e Programas") as pbar:
            for member in tar:
                tar.extract(member, path=str(DESTINO_DIR))
                pbar.set_postfix_str(f"{member.name[:30]}", refresh=False)
                pbar.update(1)

    reader.close()
    duracao = time.time() - inicio

    # Garante que pastas estruturais obrigatórias existam mesmo em backups antigos
    garantir_estrutura_postgres(DESTINO_DIR)

    print("\n" + "=" * 75)
    print("RESTAURAÇÃO CONCLUÍDA COM SUCESSO!")
    print(f"Tempo total de extração: {duracao / 60:.1f} minutos")
    print(f"Tanto 'E:\\PostgreSQL' quanto 'E:\\Programas' foram restaurados com sucesso em: {DESTINO_DIR}")
    print("Diretórios estruturais do PostgreSQL validados.")
    print("=" * 75)

def garantir_estrutura_postgres(destino_dir: pathlib.Path):
    data_dir = destino_dir / "PostgreSQL" / "data"
    if not data_dir.exists():
        return
    subdirs_obrigatorios = [
        "pg_commit_ts",
        "pg_dynshmem",
        "pg_logical/mappings",
        "pg_logical/snapshots",
        "pg_notify",
        "pg_replslot",
        "pg_serial",
        "pg_snapshots",
        "pg_stat_tmp",
        "pg_tblspc",
        "pg_twophase",
        "pg_wal/archive_status"
    ]
    for sub in subdirs_obrigatorios:
        (data_dir / sub).mkdir(parents=True, exist_ok=True)

if __name__ == "__main__":
    executar_restauracao()
